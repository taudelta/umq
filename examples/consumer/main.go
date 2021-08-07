package main

import (
	"flag"

	"github.com/streadway/amqp"
	log "github.com/taudelta/nanolog"
	"github.com/taudelta/umq"
)

func GetHandler(m []amqp.Delivery) error {
	m[0].Ack(true)
	return nil
}

func main() {

	var (
		targetQueue    string
		targetExchange string
		workersCount   int
		prefetchCount  int
		wait           int

		user     string
		password string
		host     string
		port     int
	)

	flag.StringVar(&targetQueue, "queue", "", "queue to push")
	flag.StringVar(&targetExchange, "exchange", "", "exchange to push")
	flag.IntVar(&workersCount, "workers", 1, "")
	flag.IntVar(&prefetchCount, "prefetch", 1, "")
	flag.IntVar(&wait, "wait", 1, "")

	flag.StringVar(&user, "user", "guest", "user")
	flag.StringVar(&password, "password", "guest", "password")
	flag.StringVar(&host, "host", "localhost", "host")
	flag.IntVar(&port, "port", 5672, "port")

	flag.Parse()

	log.Init(log.Options{
		Level: log.DebugLevel,
	})

	logger := log.DefaultLogger()

	consumer, err := umq.NewConsumer(&umq.ConsumeOptions{
		Options: umq.Options{
			Dial: umq.DialOptions{
				User:     user,
				Password: password,
				Host:     host,
				Port:     port,
			},
			Queue: umq.QueueOptions{
				Name:    targetQueue,
				Durable: true,
			},
			Exchange: umq.ExchangeOptions{
				Name:    targetExchange,
				Kind:    umq.DirectKind,
				Durable: true,
			},
			Bind: umq.BindOptions{
				RoutingKey: targetQueue,
			},
		},
		NumOfWorkers:  workersCount,
		PrefetchCount: 1,
		Logger:        logger,
	})

	if err != nil {
		log.Fatal().Println(err)
	}

	logger.Debug().Println("start consumer")

	consumer.Run(&umq.WorkerOptions{
		Handler: &umq.QueueHandler{
			Manual: true,
			Apply:  umq.ManualHandler(GetHandler),
		},
		WaitTimeSeconds: 1,
		Logger:          logger,
	}, 0)

}
