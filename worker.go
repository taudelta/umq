package umq

import (
	"sync"
	"time"

	"github.com/streadway/amqp"
	log "github.com/taudelta/nanolog"
)

type WorkerOptions struct {
	Handler         *QueueHandler
	WaitTimeSeconds int
	Logger          *log.NanoLogger
}

type Worker struct {
	connector         *Connector
	consumerOptions   *ConsumeOptions
	workerOptions     *WorkerOptions
	shutdownWaitGroup *sync.WaitGroup
	logger            *log.NanoLogger
}

func NewWorker(consumerOptions *ConsumeOptions, workerOptions *WorkerOptions, errorCh chan *Worker) (*Worker, error) {

	worker := &Worker{
		consumerOptions:   consumerOptions,
		workerOptions:     workerOptions,
		shutdownWaitGroup: consumerOptions.ShutdownWaitGroup,
		logger:            consumerOptions.Logger,
	}

	deliveries, bufferedChannel, err := startBufferedConsume(
		worker, consumerOptions, workerOptions)
	if err != nil {
		return nil, err
	}

	go worker.onDelivery(deliveries, bufferedChannel, errorCh)

	return worker, err
}

func startBufferedConsume(worker *Worker, consumerOptions *ConsumeOptions, workerOptions *WorkerOptions) (<-chan amqp.Delivery, chan amqp.Delivery, error) {

	if workerOptions.Handler == nil {
		consumerOptions.Logger.Fatal().Println("handler not implemented")
	}

	connector, deliveries, err := newConsumerConnector(worker.consumerOptions)
	if err != nil {
		return nil, nil, err
	}

	worker.connector = connector

	completeCh := make(chan bool, 1)
	go worker.waitShutdown(connector, completeCh, consumerOptions.ShutdownWaitGroup, consumerOptions.CloseTimeout)

	bufferedChannel := worker.consume(completeCh)

	return deliveries, bufferedChannel, err
}

func (w *Worker) consume(completeCh chan bool) chan amqp.Delivery {

	prefetchCount := w.consumerOptions.PrefetchCount
	buffer := make([]amqp.Delivery, 0, prefetchCount)
	ticker := time.NewTicker(time.Duration(w.workerOptions.WaitTimeSeconds) * time.Second)
	pendingBuffer := []amqp.Delivery{}

	bufferedChannel := make(chan amqp.Delivery, prefetchCount)

	go func() {
		for {

			select {
			case <-w.connector.terminateCh:
				w.logger.Debug().Println("terminate worker")
				completeCh <- true
				close(w.connector.terminateCh)
				return
			case d := <-bufferedChannel:
				if w.consumerOptions.AutoAck && len(buffer) == w.consumerOptions.PrefetchCount {
					pendingBuffer = append(pendingBuffer, d)
				} else {
					buffer = append(buffer, d)
				}

				if len(buffer) >= prefetchCount {
					pendingBuffer = w.sendBuffer(buffer, pendingBuffer)
					buffer = make([]amqp.Delivery, 0, prefetchCount)
					ticker.Stop()
					ticker = time.NewTicker(time.Duration(w.workerOptions.WaitTimeSeconds) * time.Second)
				}

			case <-ticker.C:
				pendingBuffer = w.sendBuffer(buffer, pendingBuffer)
				buffer = make([]amqp.Delivery, 0, prefetchCount)
			}
		}
	}()

	return bufferedChannel
}

func (w *Worker) onDelivery(deliveries <-chan amqp.Delivery, bufferedChannel chan amqp.Delivery, errorCh chan *Worker) {

	// блокировка в ожидании новых сообщений
	for d := range deliveries {
		w.logger.Debug().Printf("get raw message: %+v\n", d)
		if w.connector.NeedShutdown {
			continue
		}
		bufferedChannel <- d
	}

	w.logger.Debug().Println("worker deliveries channel closed")

	if w.connector.NeedShutdown {
		w.logger.Debug().Println("successfull shutdown")
		return
	}

	w.logger.Debug().Println("failure shutdown")
	select {
	case _, ok := <-errorCh:
		if ok {
			errorCh <- w
		}
	case <-time.After(100 * time.Millisecond):
		errorCh <- w
	}
}

func (w *Worker) sendBuffer(buffer []amqp.Delivery, pendingBuffer []amqp.Delivery) []amqp.Delivery {

	if w.consumerOptions.AutoAck {

		if len(pendingBuffer) >= MAX_PENDING_AUTO_ACK_COUNT {
			w.logger.Info().Println("max pending auto ack count reached %s\n", w.consumerOptions.Options.Queue.Name)
		}

		bf := []amqp.Delivery{}

		for i := 0; i < len(pendingBuffer); i++ {
			if len(buffer) < w.consumerOptions.PrefetchCount {
				buffer = append(buffer, pendingBuffer[i])
			} else {
				bf = append(bf, pendingBuffer[i])
			}
		}

		pendingBuffer = bf

	}

	if len(buffer) > 0 {
		w.handleLock(buffer)
	}

	return pendingBuffer
}

func (w *Worker) handleLock(buffer []amqp.Delivery) {

	w.connector.execLock.Lock()
	defer w.connector.execLock.Unlock()

	w.workerOptions.Handler.Apply(buffer)
}

func (w *Worker) onShutdown(channel chan bool, shutdownWg *sync.WaitGroup) {
	close(channel)
	shutdownWg.Done()
}

func (w *Worker) waitShutdown(connector *Connector, completeCh chan bool, shutdownWg *sync.WaitGroup, shutdownTimeout time.Duration) {

	for {

		if !connector.NeedShutdown {
			time.Sleep(time.Second)
			continue
		}

		select {
		case <-completeCh:
			// executing when worker receive terminate event and complete last message handling
			w.logger.Info().Printf("Worker(%p) - complete(shutdown)", connector)
			w.onShutdown(completeCh, shutdownWg)
			return

		case <-time.After(shutdownTimeout):

			w.logger.Info().Printf("Worker(%p) - timeout shutdown after %v", connector, shutdownTimeout)
			if connector.tryExecLock() {
				w.logger.Info().Printf("Worker(%p) - complete", connector)
				w.onShutdown(completeCh, shutdownWg)
				return
			}

			w.logger.Error().Printf("Worker(%p) - worker busy after %v", connector, shutdownTimeout)
		}

	}
}
