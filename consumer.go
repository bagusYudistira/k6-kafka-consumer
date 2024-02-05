package consumer

import (
	"fmt"
	"github.com/IBM/sarama"
	"go.k6.io/k6/js/modules"
)

type Config struct {
	Broker  string
	Topic   string
	GroupId string
}

type Message struct {
	Key         string
	Value       string
	HeaderKey   string
	HeaderValue string
}

type Kafka struct {
	Config   Config
	Producer sarama.AsyncProducer
	Admin    sarama.ClusterAdmin
	Client   sarama.Client
}

type Partition struct {
	Topic       *string
	Partition   int32
	Offset      int64
	Metadata    *string
	Error       int
	LeaderEpoch *int32
}

var (
	enqueued, producerErrors int
)

func (k *Kafka) Start(broker string, topic string, groupId string) *Kafka {
	config := Config{
		Broker:  broker,
		Topic:   topic,
		GroupId: groupId,
	}

	client, err := sarama.NewClient([]string{config.Broker}, sarama.NewConfig())
	if err != nil {
		fmt.Printf("Error creating consumer: %v\n", err)
		panic(err)
	}

	producer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		fmt.Printf("Error creating producer: %v\n", err)
		panic(err)
	}

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		fmt.Printf("Error creating admin client: %v\n", err)
		panic(err)
	}

	return &Kafka{
		Config:   config,
		Producer: producer,
		Admin:    admin,
		Client:   client,
	}

}

func (k *Kafka) Send(messages []Message) {
	for _, value := range messages {
		message := &sarama.ProducerMessage{
			Topic: k.Config.Topic,
			Key:   sarama.StringEncoder(value.Key),
			Value: sarama.StringEncoder(value.Value),
			Headers: []sarama.RecordHeader{
				{Key: []byte(value.HeaderKey), Value: []byte(value.HeaderValue)},
			},
		}

		select {
		case k.Producer.Input() <- message:
			enqueued++
		case err := <-k.Producer.Errors():
			fmt.Printf("Failed to produce message %v\n", err)
			producerErrors++
		}
	}
}

func (k *Kafka) Close() {
	err := k.Producer.Close()
	if err != nil {
		fmt.Printf("error to close Producer %v\n", err)
	}

	err = k.Admin.Close()
	if err != nil {
		fmt.Printf("error close Admin %v\n", err)
	}

	if !k.Client.Closed() {
		err = k.Client.Close()
		if err != nil {
			fmt.Printf("error close Client %v\n", err)
		}
	}
}

func (k *Kafka) LatestOffsetInGroup() int64 {
	// we assume that we just have a single partition
	partition := int32(0)
	latestOffset, err := k.Client.GetOffset(k.Config.Topic, partition, sarama.OffsetNewest)
	if err != nil {
		fmt.Printf("Error getting latest offset: %v\n", err)
		panic(err)
	}

	return latestOffset
}

func (k *Kafka) CommittedOffset() Partition {
	res, err := k.Admin.ListConsumerGroupOffsets(k.Config.GroupId, map[string][]int32{
		k.Config.Topic: {0},
	})

	if err != nil {
		fmt.Printf("Failed to list consumer group offsets %v\n", err)
		panic(err)
	}

	listPartition := []Partition{}
	for topic, partitionMap := range res.Blocks {
		if topic == k.Config.Topic {
			for partition, offsetBlock := range partitionMap {
				listPartition = append(listPartition, Partition{
					Topic:       &topic,
					Partition:   partition,
					Offset:      offsetBlock.Offset,
					Metadata:    &offsetBlock.Metadata,
					Error:       int(offsetBlock.Err),
					LeaderEpoch: &offsetBlock.LeaderEpoch,
				})
			}
		}
	}

	if len(listPartition) == 0 {
		return Partition{}
	}

	return listPartition[0]
}

func init() {
	modules.Register("k6/x/k6-kafka-consumer", New())
}

func New() *Kafka {
	return &Kafka{}
}
