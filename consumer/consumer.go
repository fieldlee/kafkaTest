package consumer

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"strings"
	"sync"
)

var (
	wg     sync.WaitGroup
	logger = log.New(os.Stderr, "[srama]", log.LstdFlags)
	topic = "firstTopic"
)

func Consumer()  {
	sarama.Logger = logger

	consumer,err := sarama.NewConsumer(strings.Split("localhost:9092", ","), nil)
	if err != nil {
		logger.Fatal(err.Error())
	}

	partitionList, err := consumer.Partitions(topic)

	fmt.Println("partitionList===================",partitionList)
	if err != nil {
		logger.Fatal(err.Error())
	}

	for partition := range partitionList {

		pc, err := consumer.ConsumePartition(topic, int32(partition), sarama.OffsetNewest)
		if err != nil {
			logger.Printf("Failed to start consumer for partition %d: %s\n", partition, err)
		}
		defer pc.AsyncClose()

		wg.Add(1)

		go func(sarama.PartitionConsumer) {
			defer wg.Done()
			for msg := range pc.Messages() {
				fmt.Printf("Partition:%d, Offset:%d, Key:%s, Value:%s", msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
				fmt.Println("msg==========")
			}
		}(pc)
	}

	wg.Wait()

	logger.Println("Done consuming topic hello")
	consumer.Close()
}


func GroupConsumer(){
	groupID := "group-1"

	config := sarama.NewConfig()

	//config.Group.Return.Notifications = true //如果启用，重新平衡通知将在通知通道上返回(默认禁用)。
	// config.Consumer.Offsets.CommitInterval = 1 * time.Second //提交更新偏移量的频率。默认为1。
	// config.Consumer.Offsets.Initial = sarama.OffsetNewest    //初始从最新的offset开始 应该是最新的或最迟的。默认为OffsetNewest。
	g,err :=sarama.NewConsumerGroup(strings.Split("localhost:9092", ","),groupID,config)
	//c, err := cluster.NewConsumer(strings.Split("localhost:9092", ","), groupID, strings.Split(topics, ","), config)
	if err != nil {
		//glog.Errorf("Failed open consumer: %v", err)
		return
	}
	defer g.Close()

	g.Consume()

	// 这是必须的
	go func(c *cluster.Consumer) {
		errors := c.Errors()
		// 通知返回在用户重新平衡期间发生的通知通道。只有在配置的Group.Return中，通知才会通过该通道发出。通知设置为true。
		noti := c.Notifications()
		for {
			select {
			case err := <-errors:
				glog.Errorln(err)
			case <-noti:
			}
		}
	}(c)

	for msg := range c.Messages() {
		fmt.Fprintf(os.Stdout, "%s/%d/%d\t%s\n", msg.Topic, msg.Partition, msg.Offset, msg.Value)
		// MarkOffset将提供的消息标记为已处理的消息
		c.MarkOffset(msg, "") //MarkOffset 并不是实时写入kafka，有可能在程序crash时丢掉未提交的offset
	}
}
