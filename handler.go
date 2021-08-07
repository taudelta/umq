package umq

import "github.com/streadway/amqp"

type SingleHandler func(messages []byte) (error, bool)
type BatchHandler func(messages [][]byte) (error, bool)
type ManualHandler func([]amqp.Delivery) error

type QueueHandler struct {
	Manual bool
	Batch  BatchHandler
	Apply  ManualHandler
}
