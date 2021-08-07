package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/taudelta/umq"
)

func main() {

	var (
		targetQueue    string
		targetExchange string
		batchSize      int
		user           string
		password       string
		host           string
		port           int
	)

	flag.StringVar(&targetQueue, "queue", "", "queue to push")
	flag.StringVar(&targetExchange, "exchange", "", "exchange to push")
	flag.IntVar(&batchSize, "batch", 10, "size of pushed batch")

	flag.StringVar(&user, "user", "guest", "user")
	flag.StringVar(&password, "password", "guest", "password")
	flag.StringVar(&host, "host", "localhost", "host")
	flag.IntVar(&port, "port", 5672, "port")

	flag.Parse()

	producer, err := umq.NewProducer(umq.Options{
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
	})

	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < batchSize; i++ {
		message := fmt.Sprintf("message_%d", i)
		log.Println("send message: ", message)
		producer.Send([]byte(message))
	}

}
