package main

import (
	"github.com/IBM/sarama"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	topic := "comments"
	worker, err := connectConsumer([]string{"localhost:29092"})
	if err != nil {
		panic(err)
	}

	consumer, err := worker.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	log.Println("consumer started")
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	msgCount := 0

	doneCh := make(chan struct{})

	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				log.Println(err)

			case msg := <-consumer.Messages():
				msgCount++
				log.Printf("Received message count : %d: | Topic (%s) | Message (%s)n", msgCount, string(msg.Topic), string(msg.Value))

			case <-sigchan:
				log.Println("Interruption detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
	log.Println("Processed ", msgCount, " messages")
	if err := worker.Close(); err != nil {
		panic(err)
	}
}

func connectConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	conn, err := sarama.NewConsumer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
