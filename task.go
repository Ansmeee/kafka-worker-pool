package workerPool

import (
	"github.com/IBM/sarama"
)

type Task struct {
	EventType string
	Msg       *sarama.ConsumerMessage
	session   sarama.ConsumerGroupSession
}

type eventTask struct {
	task     *Task
	handler  func(task *Task) error
	callback func(offset int64, session sarama.ConsumerGroupSession, err error)
}
