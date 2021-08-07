# UMQ

Utility package for working with RabbitMQ message broker

## Motivation

Package for working with amqp in Golang has a low-level functionality and produces a lot of boilerplate code. Because of that umq tries to reduces repeatable patterns and provide nice looking api

## Features
- Parallel processing with goroutine workers
- Buffered processing mode
- Batch and single messages processing
- Auto reconnection on failure
