package producer

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"strings"
	"time"
)

var (
	topic = "firstTopic"
	logger = log.New(os.Stderr, "[srama]", log.LstdFlags)
)

func Producer(){


	sarama.Logger = logger

	config := sarama.NewConfig()
	config.Producer.MaxMessageBytes = 1024
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	producer,err := sarama.NewSyncProducer(strings.Split("localhost:9092",","),config)
	if err != nil {
		log.Fatal(err.Error())
	}

	defer producer.Close()

	for i:=0;i<1000 ;i++  {
		msg := &sarama.ProducerMessage{}
		msg.Topic = topic
		msg.Key = sarama.StringEncoder(fmt.Sprintf("key---%d",i))
		msg.Value = sarama.ByteEncoder(fmt.Sprintf("hello infomation for kafka for key %d",i))
		msg.Partition = int32(4)
		msg.Timestamp = time.Now()

		partition,offset,err := producer.SendMessage(msg)
		if err != nil {
			log.Fatal(err.Error())
		}

		logger.Printf("partition=%d, offset=%d\n", partition, offset)

		<- time.After(1*time.Second)
	}

}