package producer

import (
	"fmt"
	"github.com/Shopify/sarama"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

func AsynProucer(){
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true //必须有这个选项
	config.Producer.Timeout = 1 * time.Second
	p, err := sarama.NewAsyncProducer(strings.Split("localhost:9092", ","), config)
	defer p.Close()
	if err != nil {
		return
	}

	//必须有这个匿名函数内容
	//循环判断哪个通道发送过来数据.
	go func(p sarama.AsyncProducer) {
		errors := p.Errors()
		success := p.Successes()
		for {
			select {
			case err := <-errors:
				if err != nil {
					fmt.Println(err.Error())
				}
			case s := <-success:
				fmt.Println(s.Topic)
				fmt.Println(s.Value)
			}
		}
	}(p)

	for {
		v := "async: " + strconv.Itoa(rand.New(rand.NewSource(time.Now().UnixNano())).Intn(10000))
		fmt.Fprintln(os.Stdout, v)
		msg := &sarama.ProducerMessage{
			Topic: topic,
			Value: sarama.ByteEncoder(v), //sarama.StringEncoder("test")
		}
		p.Input() <- msg
		time.Sleep(time.Second * 1)
	}

}