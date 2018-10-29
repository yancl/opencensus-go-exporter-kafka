package kafka

import (
	"github.com/Shopify/sarama"
)

// Mock the kafka async producer
type mockAsyncProducer struct {
	c chan<- *sarama.ProducerMessage
}

func (p *mockAsyncProducer) AsyncClose()  {}
func (p *mockAsyncProducer) Close() error { return nil }
func (p *mockAsyncProducer) Input() chan<- *sarama.ProducerMessage {
	return p.c
}
func (p *mockAsyncProducer) Successes() <-chan *sarama.ProducerMessage { return nil }
func (p *mockAsyncProducer) Errors() <-chan *sarama.ProducerError      { return nil }

func newMockAsyncProducer(c chan<- *sarama.ProducerMessage) sarama.AsyncProducer {
	return &mockAsyncProducer{c: c}
}
