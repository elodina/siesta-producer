package producer

import (
	"fmt"
	"log"
	"time"

	"github.com/elodina/siesta"
	"github.com/yanzay/cfg"
)

type ProducerRecord struct {
	Topic     string
	Partition int32
	Key       interface{}
	Value     interface{}

	encodedKey   []byte
	encodedValue []byte
	metadataChan chan *RecordMetadata
}

type RecordMetadata struct {
	Record    *ProducerRecord
	Offset    int64
	Topic     string
	Partition int32
	Error     error
}

type ProducerConfig struct {
	Partitioner       Partitioner
	MetadataExpire    time.Duration
	CompressionType   string
	BatchSize         int
	Linger            time.Duration
	Retries           int
	RetryBackoff      time.Duration
	BlockOnBufferFull bool

	ClientID        string
	MaxRequests     int
	SendRoutines    int
	ReceiveRoutines int
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	RequiredAcks    int
	AckTimeoutMs    int32
	BrokerList      []string
}

func NewProducerConfig() *ProducerConfig {
	return &ProducerConfig{
		Partitioner:     NewHashPartitioner(),
		MetadataExpire:  time.Minute,
		BatchSize:       1000,
		ClientID:        "siesta",
		MaxRequests:     10,
		SendRoutines:    10,
		ReceiveRoutines: 10,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		RequiredAcks:    1,
		AckTimeoutMs:    1000,
		Linger:          1 * time.Second,
	}
}

type Serializer func(interface{}) ([]byte, error)

func ByteSerializer(value interface{}) ([]byte, error) {
	if value == nil {
		return nil, nil
	}

	if array, ok := value.([]byte); ok {
		return array, nil
	}

	return nil, fmt.Errorf("Can't serialize %v", value)
}

func StringSerializer(value interface{}) ([]byte, error) {
	if str, ok := value.(string); ok {
		return []byte(str), nil
	}

	return nil, fmt.Errorf("Can't serialize %v to string", value)
}

type Producer interface {
	// Send the given record asynchronously and return a channel which will eventually contain the response information.
	Send(*ProducerRecord) <-chan *RecordMetadata

	// Tries to close the producer cleanly.
	Close()
}

type KafkaProducer struct {
	config            *ProducerConfig
	time              time.Time
	keySerializer     Serializer
	valueSerializer   Serializer
	messagesChan      chan *ProducerRecord
	accumulatorConfig *RecordAccumulatorConfig
	connector         siesta.Connector
	metadata          *Metadata
}

func NewKafkaProducer(config *ProducerConfig, keySerializer Serializer, valueSerializer Serializer, connector siesta.Connector) *KafkaProducer {
	log.Println("Starting the Kafka producer")
	producer := &KafkaProducer{}
	producer.config = config
	producer.time = time.Now()
	producer.messagesChan = make(chan *ProducerRecord, config.BatchSize)
	producer.keySerializer = keySerializer
	producer.valueSerializer = valueSerializer
	producer.connector = connector
	producer.metadata = NewMetadata(connector, config.MetadataExpire)

	client := NewNetworkClient(connector, config)

	producer.accumulatorConfig = &RecordAccumulatorConfig{
		batchSize:     config.BatchSize,
		linger:        config.Linger,
		networkClient: client,
	}
	go producer.messageDispatchLoop()

	log.Println("Kafka producer started")

	return producer
}

func ProducerConfigFromFile(filename string) (*ProducerConfig, error) {
	c, err := cfg.LoadNewMap(filename)
	if err != nil {
		return nil, err
	}

	producerConfig := NewProducerConfig()
	if err := setDurationConfig(&producerConfig.MetadataExpire, c["metadata.max.age"]); err != nil {
		return nil, err
	}
	if err := setIntConfig(&producerConfig.BatchSize, c["batch.size"]); err != nil {
		return nil, err
	}
	if err := setIntConfig(&producerConfig.RequiredAcks, c["acks"]); err != nil {
		return nil, err
	}
	if err := setInt32Config(&producerConfig.AckTimeoutMs, c["timeout.ms"]); err != nil {
		return nil, err
	}
	if err := setDurationConfig(&producerConfig.Linger, c["linger"]); err != nil {
		return nil, err
	}
	setStringConfig(&producerConfig.ClientID, c["client.id"])
	if err := setIntConfig(&producerConfig.SendRoutines, c["send.routines"]); err != nil {
		return nil, err
	}
	if err := setIntConfig(&producerConfig.ReceiveRoutines, c["receive.routines"]); err != nil {
		return nil, err
	}
	setBoolConfig(&producerConfig.BlockOnBufferFull, c["block.on.buffer.full"])
	if err := setIntConfig(&producerConfig.Retries, c["retries"]); err != nil {
		return nil, err
	}
	if err := setDurationConfig(&producerConfig.RetryBackoff, c["retry.backoff"]); err != nil {
		return nil, err
	}
	setStringConfig(&producerConfig.CompressionType, c["compression.type"])
	if err := setIntConfig(&producerConfig.MaxRequests, c["max.requests"]); err != nil {
		return nil, err
	}

	setStringsConfig(&producerConfig.BrokerList, c["bootstrap.servers"])
	if len(producerConfig.BrokerList) == 0 {
		setStringsConfig(&producerConfig.BrokerList, c["metadata.broker.list"])
	}

	return producerConfig, nil
}

func (kp *KafkaProducer) Send(record *ProducerRecord) <-chan *RecordMetadata {
	record.metadataChan = make(chan *RecordMetadata, 1)
	kp.send(record)
	return record.metadataChan
}

func (kp *KafkaProducer) send(record *ProducerRecord) {
	metadataChan := record.metadataChan
	metadata := new(RecordMetadata)

	serializedKey, err := kp.keySerializer(record.Key)
	if err != nil {
		metadata.Error = err
		metadataChan <- metadata
		return
	}

	serializedValue, err := kp.valueSerializer(record.Value)
	if err != nil {
		metadata.Error = err
		metadataChan <- metadata
		return
	}

	record.encodedKey = serializedKey
	record.encodedValue = serializedValue

	partitions, err := kp.metadata.Get(record.Topic)
	if err != nil {
		metadata.Error = err
		metadataChan <- metadata
		return
	}

	partition, err := kp.config.Partitioner.Partition(record, partitions)
	if err != nil {
		metadata.Error = err
		metadataChan <- metadata
		return
	}
	record.Partition = partition

	kp.messagesChan <- record
}

func (kp *KafkaProducer) messageDispatchLoop() {
	accumulators := make(map[string]chan *ProducerRecord)
	for message := range kp.messagesChan {
		accumulator := accumulators[message.Topic]
		if accumulator == nil {
			accumulator = make(chan *ProducerRecord, kp.config.BatchSize)
			accumulators[message.Topic] = accumulator
			go kp.topicDispatchLoop(accumulator)
		}

		accumulator <- message
	}

	for _, accumulator := range accumulators {
		close(accumulator)
	}
}

func (kp *KafkaProducer) topicDispatchLoop(topicMessagesChan chan *ProducerRecord) {
	accumulators := make(map[int32]*RecordAccumulator)
	for message := range topicMessagesChan {
		accumulator := accumulators[message.Partition]
		if accumulator == nil {
			accumulator = NewRecordAccumulator(kp.accumulatorConfig)
			accumulators[message.Partition] = accumulator
		}

		accumulator.input <- message
	}

	for _, accumulator := range accumulators {
		close(accumulator.closeChan)
	}
}

func (kp *KafkaProducer) Close() {
	close(kp.messagesChan)
}
