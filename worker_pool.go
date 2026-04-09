package workerPool

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type WorkerPool struct {
	mu       sync.RWMutex
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	stopOnce sync.Once

	partitions  map[int32]*partition
	taskHandler func(task *Task) error

	workPoolConfig WorkerPoolConfig
	kafkaConfig    KafkaConfig
	consumerGroup  sarama.ConsumerGroup
	consumerHandle sarama.ConsumerGroupHandler
}

type WorkerPoolConfig struct {
	KafkaConfig KafkaConfig
	Handler     func(task *Task) error
	MaxTaskSize int64
}

type KafkaConfig struct {
	Brokers []string
	Topics  []string
	Group   string
}

func NewWorkerPool(config WorkerPoolConfig) (*WorkerPool, error) {
	kc := sarama.NewConfig()
	kc.Consumer.Return.Errors = true
	kc.Version = sarama.V2_0_0_0
	kc.Consumer.Offsets.Initial = sarama.OffsetOldest

	group, err := sarama.NewConsumerGroup(config.KafkaConfig.Brokers, config.KafkaConfig.Group, kc)
	if err != nil {
		return nil, fmt.Errorf("consumer group initial error: %s", err.Error())
	}

	ctx, cancel := context.WithCancel(context.Background())
	wp := &WorkerPool{
		ctx:            ctx,
		cancel:         cancel,
		wg:             sync.WaitGroup{},
		mu:             sync.RWMutex{},
		partitions:     make(map[int32]*partition),
		taskHandler:    config.Handler,
		consumerGroup:  group,
		kafkaConfig:    config.KafkaConfig,
		workPoolConfig: config,
	}

	wp.consumerHandle = &ConsumerHandler{wp: wp}
	return wp, nil
}

func (wp *WorkerPool) Start() {
	go func() {
		for err := range wp.consumerGroup.Errors() {
			fmt.Println("consumer group error:", err.Error())
		}
	}()

	for {
		select {
		case <-wp.ctx.Done():
			return
		default:
			if err := wp.consumerGroup.Consume(wp.ctx, wp.kafkaConfig.Topics, wp.consumerHandle); err != nil {
				fmt.Println("consumer error:", err.Error())
				time.Sleep(1 * time.Millisecond)
			}
		}
	}
}

func (wp *WorkerPool) Stop() {

}

func (wp *WorkerPool) dispatch(msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) error {
	part, ok := wp.partitions[msg.Partition]
	if !ok {
		part = &partition{
			ctx:           wp.ctx,
			partition:     msg.Partition,
			topic:         msg.Topic,
			taskQueue:     make(chan *Task, wp.workPoolConfig.MaxTaskSize),
			dispatcher:    &dispatcher{ctx: wp.ctx, maxTaskSize: wp.workPoolConfig.MaxTaskSize, workers: make(map[string]*worker), handler: wp.taskHandler},
			offsetTracker: &offsetTracker{completed: make(map[int64]bool), initialized: false},
		}

		wp.partitions[msg.Partition] = part
		go part.consume()
	}

	part.taskQueue <- part.generateTask(msg, session)

	return nil
}
