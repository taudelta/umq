package umq

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/streadway/amqp"
)

const (
	DirectKind = "direct"
	FanoutKind = "fanout"
)

type RabbitProducer struct {
	options Options
	c       *Connector
}

const defaultTimeout = 500 * time.Millisecond

func (p *RabbitProducer) Send(message []byte) error {
	return p.Json(message, defaultTimeout, nil)
}

func (p *RabbitProducer) SendWithOptions(message []byte, options *MessageOptions) error {
	return p.Json(message, defaultTimeout, options)
}

func (p *RabbitProducer) Shutdown() error {

	if p.c == nil {
		return nil
	}

	if p.c.ch != nil {
		if err := p.c.ch.Close(); err != nil {
			return err
		}
	}

	if p.c.cn != nil {
		if err := p.c.cn.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (p *RabbitProducer) tryLock(timeout time.Duration) error {

	ch := make(chan struct{})

	go func() {
		p.c.RLock()
		defer p.c.RUnlock()
		close(ch)
	}()

	select {
	case <-ch:
		return nil
	case <-time.After(timeout):

		select {
		case _, ok := <-ch:
			if ok {
				close(ch)
			}
		}

		return errors.New(fmt.Sprintf("Producer(%p) - can't send message, reconnecting", p))
	}
}

func (p *RabbitProducer) send(message amqp.Publishing, timeout time.Duration, options *MessageOptions) error {

	var err error

	if p.c == nil {
		err = errors.New(fmt.Sprintf("Producer(%p) - can't send message, channel is not initialized", p))
		return err
	}

	if err := p.tryLock(time.Second); err != nil {
		return err
	}

	err = sendMessageWithRetry(p.c.ch, p.options.Exchange.Name, p.options.Queue.Name, message, timeout, 10, options)
	if err != nil {
		return err
	}

	return nil
}

func sendMessageWithRetry(channel *amqp.Channel, exchangeName, routingKey string, msg amqp.Publishing,
	timeout time.Duration, retryCount int, options *MessageOptions) (err error) {

	if options == nil {
		options = &MessageOptions{}
	}

	resendCnt := 0

	for {

		err = errors.New("channel is not accessible")

		if options.Priority > 0 {
			msg.Priority = uint8(options.Priority)
		}

		if options.TTL > 0 {
			msg.Expiration = strconv.Itoa(int(options.TTL.Seconds() * 1000))
		}

		if channel != nil {
			err = channel.Publish(
				exchangeName,
				routingKey,
				options.Mandatory,
				options.Immediate,
				msg,
			)
		}

		if err == nil {
			return
		}

		timeout += 100 * time.Millisecond
		resendCnt += 1

		if resendCnt > retryCount {
			return
		}

		time.Sleep(timeout)
	}

}

func (p *RabbitProducer) Json(body []byte, timeout time.Duration, options *MessageOptions) error {
	msg := NewMessage(body, "application/json", "utf-8")
	return p.send(msg, timeout, options)
}

func NewProducer(options Options) (*RabbitProducer, error) {

	connector, err := createPublisherConnector(options)
	if err != nil {
		return nil, err
	}

	producer := &RabbitProducer{
		options: options,
		c:       connector,
	}

	return producer, nil

}

func connectToQueue(options Options) (*Connector, error) {

	connectionCounter := 0
	timeout := 1

	for {

		connector, err := setupQueue(options)
		if err == nil {
			return connector, nil
		}

		time.Sleep(time.Duration(timeout) * time.Second)
		connectionCounter += 1
		timeout += 1

		if connectionCounter > 10 {
			return nil, errors.New("producer not created")
		}
	}

}

func resetConnector(src, dst *Connector) {

	src.Lock()
	defer src.Unlock()

	src.ch = dst.ch
	src.cn = dst.cn
	src.errorCh = make(chan *amqp.Error, 1)
	src.ch.NotifyClose(src.errorCh)

}

func tryConnect(connector *Connector, options Options) (*Connector, error) {

	for {
		newConnector, err := connectToQueue(options)
		if err == nil {
			resetConnector(connector, newConnector)
			return connector, nil
		}

		time.Sleep(time.Second)
	}

}

func onCheckChannelErrors(connector *Connector, options Options) {

	for range connector.errorCh {

		tryConnect(connector, options)

		go onCheckChannelErrors(connector, options)
	}
}

func createPublisherConnector(options Options) (*Connector, error) {

	connector, err := connectToQueue(options)
	if err != nil {
		return nil, err
	}

	errorCh := make(chan *amqp.Error, 1)
	connector.errorCh = errorCh
	if connector.ch != nil {
		connector.ch.NotifyClose(errorCh)
	}

	go onCheckChannelErrors(connector, options)

	return connector, nil
}
