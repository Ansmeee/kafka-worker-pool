package workerPool

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
)

func TestPartition_Consume_Dispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	session := newMockSession()
	// 创建测试分区
	p := &partition{
		ctx:           ctx,
		partition:     0,
		topic:         "test",
		taskQueue:     make(chan *Task, 10),
		offsetTracker: &offsetTracker{completed: make(map[int64]bool)},
	}

	// 创建简易 dispatcher
	dp := &dispatcher{
		ctx:         ctx,
		wg:          &sync.WaitGroup{},
		maxTaskSize: 100,
		workers:     make(map[string]*worker),
		handler:     func(t *Task) error { return nil },
	}
	p.dispatcher = dp

	// 启动消费循环
	go p.consume()

	// 模拟消息到达
	msg1 := &sarama.ConsumerMessage{Offset: 1, Partition: 0, Topic: "test"}
	task1 := p.generateTask(msg1, session)
	p.taskQueue <- task1

	// 等待处理
	time.Sleep(50 * time.Millisecond)

	// 由于 handler 直接成功，offset 应被标记
	assert.Equal(t, int64(1), p.offsetTracker.committedOffset)

	// 停止消费
	cancel()
}

func TestPartition_OnCompletion(t *testing.T) {
	p := &partition{
		topic:         "test",
		partition:     0,
		offsetTracker: &offsetTracker{completed: make(map[int64]bool)},
	}
	p.offsetTracker.init(5)
	session := newMockSession()

	p.onCompletion(5, session)
	marked, ok := session.GetMarkedOffset("test", 0)
	assert.True(t, ok)
	assert.Equal(t, int64(6), marked)
	assert.Equal(t, int64(5), p.offsetTracker.committedOffset)
}

func TestPartition_OnError_DeadQueue(t *testing.T) {
	deadQueue := make(chan *Task, 1)
	p := &partition{
		deadQueue: deadQueue,
	}

	task := &Task{Msg: &sarama.ConsumerMessage{Offset: 100}}
	p.onError(task)

	select {
	case received := <-deadQueue:
		assert.Equal(t, int64(100), received.Msg.Offset)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("deadQueue did not receive task")
	}
}
