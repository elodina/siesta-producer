package producer

import (
	"fmt"
	"github.com/elodina/siesta"
	"net"
)

type NetworkClient struct {
	connector               siesta.Connector
	metadata                Metadata
	socketSendBuffer        int
	socketReceiveBuffer     int
	clientId                string
	nodeIndexOffset         int
	correlation             int
	metadataFetchInProgress bool
	lastNoNodeAvailableMs   int64
	selector                *Selector
	connections             map[string]*net.TCPConn
	requiredAcks            int
	ackTimeoutMs            int32
}

type NetworkClientConfig struct {
}

func NewNetworkClient(config NetworkClientConfig, connector siesta.Connector, producerConfig *ProducerConfig) *NetworkClient {
	client := &NetworkClient{}
	client.connector = connector
	client.requiredAcks = producerConfig.RequiredAcks
	client.ackTimeoutMs = producerConfig.AckTimeoutMs
	selectorConfig := NewSelectorConfig(producerConfig)
	client.selector = NewSelector(selectorConfig)
	client.connections = make(map[string]*net.TCPConn, 0)
	return client
}

func (nc *NetworkClient) send(batch []*ProducerRecord) {
	if len(batch) == 0 {
		Logger.Warn("NetworkClient received an empty batch?")
		return
	}

	msg := batch[0]
	topic := msg.Topic
	partition := msg.Partition

	leader, err := nc.connector.GetLeader(topic, partition)
	if err != nil {
		for _, record := range batch {
			record.metadataChan <- &RecordMetadata{Record: record, Error: err}
		}
	}

	request := new(siesta.ProduceRequest)
	request.RequiredAcks = int16(nc.requiredAcks)
	request.AckTimeoutMs = nc.ackTimeoutMs
	for _, record := range batch {
		request.AddMessage(record.Topic, record.Partition, &siesta.Message{Key: record.encodedKey, Value: record.encodedValue})
	}
	responseChan := nc.selector.Send(leader, request)

	go nc.listenForResponse(topic, partition, batch, responseChan)
}

func (nc *NetworkClient) listenForResponse(topic string, partition int32, batch []*ProducerRecord, responseChan <-chan *rawResponseAndError) {
	response := <-responseChan

	if response.sendErr != nil {
		nc.connector.RefreshMetadata([]string{topic})
		for _, record := range batch {
			record.metadataChan <- &RecordMetadata{Record: record, Error: fmt.Errorf("Got an connection error while sending: %s", response.sendErr.Error())}
		}
		return
	}

	if nc.requiredAcks == 0 {
		// acks = 0 case, just complete all requests
		for _, record := range batch {
			record.metadataChan <- &RecordMetadata{
				Record:    record,
				Offset:    -1,
				Topic:     topic,
				Partition: partition,
				Error:     siesta.ErrNoError,
			}
		}
		return
	}

	if response.receiveErr != nil {
		nc.connector.RefreshMetadata([]string{topic})
		for _, record := range batch {
			record.metadataChan <- &RecordMetadata{Record: record, Error: fmt.Errorf("Got an connection error while receiving: %s", response.receiveErr.Error())}
		}
		return
	}

	decoder := siesta.NewBinaryDecoder(response.bytes)
	produceResponse := new(siesta.ProduceResponse)
	decodingErr := produceResponse.Read(decoder)
	if decodingErr != nil {
		for _, record := range batch {
			record.metadataChan <- &RecordMetadata{Record: record, Error: decodingErr.Error()}
		}
		return
	}

	status, exists := produceResponse.Status[topic][partition]
	if exists {
		if status.Error == siesta.ErrNotLeaderForPartition {
			nc.connector.RefreshMetadata([]string{topic})
		}

		currentOffset := status.Offset
		for _, record := range batch {
			record.metadataChan <- &RecordMetadata{
				Record:    record,
				Topic:     topic,
				Partition: partition,
				Offset:    currentOffset,
				Error:     status.Error,
			}
			currentOffset++
		}
	}

}

func (nc *NetworkClient) close() {
	nc.selector.Close()
}
