package producer

import "time"

type RecordAccumulatorConfig struct {
	batchSize     int
	linger        time.Duration
	networkClient *NetworkClient
}

type RecordAccumulator struct {
	input chan *ProducerRecord

	config        *RecordAccumulatorConfig
	networkClient *NetworkClient
	batchSize     int

	closing chan bool
	closed  chan bool
}

func NewRecordAccumulator(config *RecordAccumulatorConfig) *RecordAccumulator {
	accumulator := &RecordAccumulator{}
	accumulator.input = make(chan *ProducerRecord, config.batchSize)
	accumulator.config = config
	accumulator.batchSize = config.batchSize
	accumulator.networkClient = config.networkClient
	accumulator.closing = make(chan bool)
	accumulator.closed = make(chan bool)

	go accumulator.sender()

	return accumulator
}

func (ra *RecordAccumulator) sender() {
	timeout := time.NewTimer(ra.config.linger)
	batch := make([]*ProducerRecord, ra.batchSize)
	batchIndex := 0
	for {
		select {
		case <-ra.closing:
			return
		default:
			select {
			case message := <-ra.input:
				{
					batch[batchIndex] = message
					batchIndex++
					if batchIndex >= ra.batchSize {
						ra.networkClient.send(batch[:batchIndex])
						batch = make([]*ProducerRecord, ra.batchSize)
						batchIndex = 0
						timeout.Reset(ra.config.linger)
					}
				}
			case <-timeout.C:
				if batchIndex > 0 {
					ra.networkClient.send(batch[:batchIndex])
					batch = make([]*ProducerRecord, ra.batchSize)
					batchIndex = 0
				}
				timeout.Reset(ra.config.linger)
			}
		}
	}
}
