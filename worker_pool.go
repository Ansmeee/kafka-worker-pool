package workerPool

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type Pool struct {
	mu       sync.RWMutex
	wg       sync.WaitGroup
	ctx      context.Context
	cancel   context.CancelFunc
	stopOnce sync.Once

	partitions  map[int32]*partition
	taskHandler func(task *Task) error

	poolConfig     PoolConfig
	kafkaConfig    KafkaConfig
	consumerGroup  sarama.ConsumerGroup
	consumerHandle sarama.ConsumerGroupHandler
}

type PoolConfig struct {
	KafkaConfig KafkaConfig
	Handler     func(task *Task) error
	MaxTaskSize int64
}

type KafkaConfig struct {
	Brokers []string
	Topics  []string
	Group   string
}

func NewPool(config PoolConfig) (*Pool, error) {
	if len(config.KafkaConfig.Brokers) == 0 || len(config.KafkaConfig.Topics) == 0 || config.KafkaConfig.Group == "" {
		return nil, fmt.Errorf("kafka config is invalid: brokers, topics, and group are required")
	}

	if config.Handler == nil {
		return nil, fmt.Errorf("task handler is required")
	}

	if config.MaxTaskSize <= 0 {
		config.MaxTaskSize = 1000
	}

	kc := sarama.NewConfig()
	kc.Consumer.Return.Errors = true
	kc.Version = sarama.V2_0_0_0
	kc.Consumer.Offsets.Initial = sarama.OffsetOldest

	group, err := sarama.NewConsumerGroup(config.KafkaConfig.Brokers, config.KafkaConfig.Group, kc)
	if err != nil {
		return nil, fmt.Errorf("consumer group initial error: %s", err.Error())
	}

	ctx, cancel := context.WithCancel(context.Background())
	wp := &Pool{
		ctx:           ctx,
		cancel:        cancel,
		wg:            sync.WaitGroup{},
		mu:            sync.RWMutex{},
		partitions:    make(map[int32]*partition),
		taskHandler:   config.Handler,
		consumerGroup: group,
		kafkaConfig:   config.KafkaConfig,
		poolConfig:    config,
	}

	wp.consumerHandle = &ConsumerHandler{wp: wp}
	return wp, nil
}

func (wp *Pool) Start() {
	wp.wg.Add(1)
	go func() {
		defer wp.wg.Done()
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

func (wp *Pool) Stop() {
	wp.stopOnce.Do(func() {
		wp.cancel()
		if wp.consumerGroup != nil {
			if err := wp.consumerGroup.Close(); err != nil {
				fmt.Println("consumer error:", err.Error())
			}
		}

		wp.wg.Wait()
	})
}

func (wp *Pool) dispatch(msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) error {
	wp.mu.Lock()
	part, ok := wp.partitions[msg.Partition]
	if !ok {
		part = &partition{
			ctx:       wp.ctx,
			partition: msg.Partition,
			topic:     msg.Topic,
			taskQueue: make(chan *Task, wp.poolConfig.MaxTaskSize),
			dispatcher: &dispatcher{
				ctx:         wp.ctx,
				wg:          &wp.wg,
				maxTaskSize: wp.poolConfig.MaxTaskSize,
				workers:     make(map[string]*worker),
				handler:     wp.taskHandler,
			},
			offsetTracker: &offsetTracker{completed: make(map[int64]bool), initialized: false},
		}

		wp.partitions[msg.Partition] = part
		wp.wg.Add(1)
		go func() {
			defer wp.wg.Done()
			part.consume()
		}()
	}
	wp.mu.Unlock()

	part.offsetTracker.init(msg.Offset)

	select {
	case <-wp.ctx.Done():
		return nil
	case part.taskQueue <- part.generateTask(msg, session):
	}

	return nil
}
