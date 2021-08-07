package umq

import (
	"time"

	"github.com/streadway/amqp"
)

type MessageOptions struct {
	TTL       time.Duration
	Priority  int64
	Mandatory bool
	Immediate bool
}

func NewMessage(body []byte, contentType, encoding string) amqp.Publishing {

	msg := amqp.Publishing{
		Timestamp:       time.Now(),
		Headers:         amqp.Table{},
		ContentType:     contentType,
		ContentEncoding: encoding,
		Body:            body,
		DeliveryMode:    amqp.Persistent,
	}

	return msg
}
