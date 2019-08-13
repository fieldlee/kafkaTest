package main

import (
	"kafka/consumer"
	"kafka/producer"
)

func main()  {
	var w chan string
	go producer.AsynProucer()
	go consumer.Consumer()

	<-w
}