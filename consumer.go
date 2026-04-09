package workerPool

import (
	"fmt"

	"github.com/IBM/sarama"
)

type ConsumerHandler struct {
	wp *Pool
}

func (c *ConsumerHandler) Setup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *ConsumerHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (c *ConsumerHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if err := c.wp.dispatch(msg, session); err != nil {
			fmt.Printf("Message topic:%q partition:%d key:%s offset:%d shard err:%s \n", msg.Topic, msg.Partition, msg.Key, msg.Offset, err.Error())
		}
	}
	return nil
}
