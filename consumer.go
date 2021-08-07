package umq

import (
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/streadway/amqp"
	log "github.com/taudelta/nanolog"
)

const MAX_PENDING_AUTO_ACK_COUNT = 100000

type ConsumeOptions struct {
	Options           Options
	NumOfWorkers      int
	PrefetchCount     int
	PrefetchSize      int
	ShutdownWaitGroup *sync.WaitGroup
	AutoAck           bool
	CloseTimeout      time.Duration
	Exclusive         bool
	NoWait            bool
	NoLocal           bool
	Args              amqp.Table
	Logger            *log.NanoLogger
}

type Consumer struct {
	Options      *ConsumeOptions
	workers      []*Worker
	ErrorCh      chan *Worker
	NeedShutdown bool
	logger       *log.NanoLogger
}

func NewConsumer(options *ConsumeOptions) (*Consumer, error) {

	if options.ShutdownWaitGroup == nil {
		options.ShutdownWaitGroup = &sync.WaitGroup{}
	}

	return &Consumer{
		workers: make([]*Worker, options.NumOfWorkers),
		ErrorCh: make(chan *Worker, options.NumOfWorkers),
		Options: options,
		logger:  options.Logger,
	}, nil
}

func newConsumerConnector(options *ConsumeOptions) (*Connector, <-chan amqp.Delivery, error) {

	queueOptions := options.Options

	connector, err := Queue(queueOptions)
	if err != nil {
		return nil, nil, err
	}

	err = connector.Ch.Qos(options.PrefetchCount, options.PrefetchSize, false)
	if err != nil {
		return nil, nil, fmt.Errorf("set qos error: %s", err)
	}

	deliveries, err := connector.Ch.Consume(
		queueOptions.Queue.Name,
		queueOptions.Exchange.Name,
		options.AutoAck,
		options.Exclusive,
		options.NoLocal,
		options.NoWait,
		options.Args,
	)

	if err != nil {
		return nil, nil, fmt.Errorf("queue consume error: %s", err)
	}

	return connector, deliveries, nil

}

func wrapHandler(handler SingleHandler) BatchHandler {
	return func(bs [][]byte) (error, bool) {
		return handler(bs[0])
	}
}

func (c *Consumer) setShutdown(doneCh chan bool) {
	if c.NeedShutdown {
		return
	}
	c.NeedShutdown = true
	close(c.ErrorCh)
	doneCh <- true
}

func waitAndStop(consumer *Consumer, stopWaitPeriod time.Duration, doneCh chan bool, signalsCh chan os.Signal) bool {

	messages := 0

	for _, worker := range consumer.workers {

		c := worker.connector
		if c == nil || c.Ch == nil {
			continue
		}

		state, err := c.Ch.QueueInspect(consumer.Options.Options.Queue.Name)
		if err != nil {
			continue
		}

		messages += state.Messages
	}

	if messages == 0 {
		go func() {
			time.Sleep(stopWaitPeriod)
			consumer.setShutdown(doneCh)
			signal.Stop(signalsCh)
		}()
		return false
	}

	return true
}

func (c *Consumer) shutdown() {

	for _, w := range c.workers {
		w.connector.terminate()
	}

	c.Options.ShutdownWaitGroup.Wait()

	for _, w := range c.workers {
		if err := w.connector.close(); err != nil {
			c.logger.Error().Printf("connector close error: %s\n", err)
		}
	}
}

func (c *Consumer) worker(workerOptions *WorkerOptions, errorCh chan *Worker) *Worker {

	var worker *Worker
	var err error

	for {
		worker, err = NewWorker(c.Options, workerOptions, errorCh)
		if err != nil {
			c.logger.Error().Println(err)
			time.Sleep(time.Second)
		} else {
			c.logger.Debug().Println("create worker")
			return worker
		}
	}

}

func (c *Consumer) Run(workerOptions *WorkerOptions, stopWait time.Duration) {

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	done := make(chan bool, 1)

	defer c.shutdown()

	go func() {
		sig := <-sigs
		c.logger.Debug().Printf("exit consumer, cause=%s", sig)
		c.setShutdown(done)
	}()

	for workerIndex := range c.workers {
		c.Options.ShutdownWaitGroup.Add(1)
		c.workers[workerIndex] = c.worker(workerOptions, c.ErrorCh)
	}

	// wait for flushing all messages from queue and then shutdown
	if stopWait > 0 {
		go func() {
			for {
				wait := waitAndStop(c, stopWait, done, sigs)
				if wait {
					time.Sleep(stopWait)
				} else {
					return
				}
			}
		}()
	}

	c.logger.Debug().Println("all workers started")
	// waiting for failed worker to reconnecting
	for failedWorker := range c.ErrorCh {
		c.logger.Debug().Println("worker channel is closed, reconnection started")
		for i, worker := range c.workers {
			if worker == failedWorker {
				c.logger.Debug().Printf("Worker(%p) - unexpected shutdown, recreate", worker)
				c.workers[i] = c.worker(workerOptions, c.ErrorCh)
			}
		}
	}

	<-done
	c.logger.Debug().Println("gracefull stop complete")
}
