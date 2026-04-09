package kafka_worker_pool

import (
	"github.com/IBM/sarama"
)

type Task struct {
	eventType string
	msg       *sarama.ConsumerMessage
	session   sarama.ConsumerGroupSession
}

type eventTask struct {
	task     *Task
	handler  func(task *Task) error
	callback func(offset int64, session sarama.ConsumerGroupSession)
}
