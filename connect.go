package umq

import (
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
)

const (
	UriTemplate = "amqp://%s:%s@%s:%d"
)

type Connector struct {
	sync.RWMutex // hold on reconnection and protects
	options      Options
	Ch           *amqp.Channel
	Cn           *amqp.Connection
	ErrorCh      chan *amqp.Error
	NeedShutdown bool
	terminateCh  chan struct{}
	shutdownWg   *sync.WaitGroup
	execLock     sync.Mutex
}

func (c *Connector) tryExecLock() bool {

	ch := make(chan struct{})

	go func() {
		c.execLock.Lock()
		defer c.execLock.Unlock()
		close(ch)
	}()

	select {
	case <-ch:
		return true
	case <-time.After(100 * time.Millisecond):
		return false
	}
}

func (c *Connector) terminate() {
	c.Lock()
	defer c.Unlock()
	c.NeedShutdown = true
	go func() {
		c.terminateCh <- struct{}{}
	}()
}

func (c *Connector) close() error {
	c.Lock()
	defer c.Unlock()
	if c.Cn == nil {
		return nil
	}
	if err := c.Cn.Close(); err != nil {
		return err
	}
	return nil
}

type DialOptions struct {
	User     string
	Password string
	Host     string
	Port     int
}

type QueueOptions struct {
	Name       string // required
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Args       amqp.Table // optional
}

type ExchangeOptions struct {
	Name       string // optional
	Kind       string // optional
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Args       amqp.Table // optional
}

type BindOptions struct {
	RoutingKey string
	NoWait     bool
	Args       amqp.Table
}

type Options struct {
	Dial     DialOptions
	Queue    QueueOptions
	Exchange ExchangeOptions
	Bind     BindOptions
}

// Queue creates connection to RabbitMQ server, declares queue and binds it to the exchange
func Queue(options Options) (*Connector, error) {

	conn, err := amqp.Dial(
		fmt.Sprintf(
			UriTemplate,
			options.Dial.User,
			options.Dial.Password,
			options.Dial.Host,
			options.Dial.Port,
		))
	if err != nil {
		return nil, err
	}

	mqChannel, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	// declare a not direct exchange if ExchangeName is provided
	if options.Exchange.Name != "" {
		err = mqChannel.ExchangeDeclare(
			options.Exchange.Name,
			options.Exchange.Kind,
			options.Exchange.Durable,
			options.Exchange.AutoDelete,
			options.Exchange.Internal,
			options.Exchange.NoWait,
			options.Exchange.Args,
		)
		if err != nil {
			return nil, err
		}
	}

	mQueue, err := mqChannel.QueueDeclare(
		options.Queue.Name,
		options.Queue.Durable,
		options.Queue.AutoDelete,
		options.Queue.Exclusive,
		options.Queue.NoWait,
		options.Queue.Args,
	)
	if err != nil {
		return nil, err
	}

	if options.Exchange.Name != "" {
		err = mqChannel.QueueBind(
			mQueue.Name,
			options.Bind.RoutingKey,
			options.Exchange.Name,
			options.Bind.NoWait,
			options.Bind.Args,
		)
		if err != nil {
			return nil, err
		}
	}

	return &Connector{
		options:     options,
		Ch:          mqChannel,
		Cn:          conn,
		terminateCh: make(chan struct{}, 1),
	}, nil
}
