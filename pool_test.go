package workerPool

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockClaim struct {
	sarama.ConsumerGroupClaim
	messages chan *sarama.ConsumerMessage
}

func newMockClaim() *mockClaim {
	return &mockClaim{
		messages: make(chan *sarama.ConsumerMessage, 10),
	}
}

func (m *mockClaim) Messages() <-chan *sarama.ConsumerMessage {
	return m.messages
}

func (m *mockClaim) Partition() int32 {
	return 0
}

func (m *mockClaim) Topic() string {
	return "test-topic"
}

func TestPool_Dispatch_NewPartition(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	wp := &Pool{
		ctx:        ctx,
		cancel:     cancel,
		partitions: make(map[int32]*partition),
		taskHandler: func(t *Task) error {
			return nil
		},
		poolConfig: PoolConfig{
			MaxTaskSize: 100,
		},
	}
	wp.wg.Add(1) // 为 dispatch 中启动的 goroutine 预留

	session := newMockSession()
	msg := &sarama.ConsumerMessage{
		Topic:     "test",
		Partition: 0,
		Offset:    10,
	}

	err := wp.dispatch(msg, session)
	require.NoError(t, err)

	// 确认分区已创建
	wp.mu.RLock()
	part, exists := wp.partitions[0]
	wp.mu.RUnlock()
	assert.True(t, exists)
	assert.NotNil(t, part)

	// 确认 task 已进入队列
	select {
	case task := <-part.taskQueue:
		assert.Equal(t, int64(10), task.Msg.Offset)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("task not dispatched")
	}

	wp.wg.Done()

	// 清理
	wp.Stop()
}

func TestPool_Dispatch_SamePartition(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	wp := &Pool{
		ctx:        ctx,
		cancel:     cancel,
		partitions: make(map[int32]*partition),
		taskHandler: func(t *Task) error {
			time.Sleep(10 * time.Millisecond) // 模拟处理
			return nil
		},
		poolConfig: PoolConfig{
			MaxTaskSize: 100,
		},
	}

	wp.wg.Add(1)
	session := newMockSession()
	// 发送两条消息到同一分区
	msg1 := &sarama.ConsumerMessage{Partition: 0, Offset: 1}
	msg2 := &sarama.ConsumerMessage{Partition: 0, Offset: 2}

	wp.dispatch(msg1, session)
	wp.dispatch(msg2, session)

	// 获取分区
	wp.mu.RLock()
	part := wp.partitions[0]
	wp.mu.RUnlock()

	// 确认队列中顺序
	task1 := <-part.taskQueue
	task2 := <-part.taskQueue
	assert.Equal(t, int64(1), task1.Msg.Offset)
	assert.Equal(t, int64(2), task2.Msg.Offset)

	wp.wg.Done()
	wp.Stop()
}

func TestPool_Stop_DrainTasks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	processed := make(chan int64, 10)
	wp := &Pool{
		ctx:        ctx,
		cancel:     cancel,
		partitions: make(map[int32]*partition),
		taskHandler: func(t *Task) error {
			processed <- t.Msg.Offset
			return nil
		},
		poolConfig: PoolConfig{MaxTaskSize: 10},
	}

	session := newMockSession()
	// 预先分发几条消息
	for i := int64(0); i < 5; i++ {
		msg := &sarama.ConsumerMessage{Partition: 0, Offset: i}
		wp.dispatch(msg, session)
	}

	// 等待一点时间让消息进入队列
	time.Sleep(50 * time.Millisecond)

	// 停止
	wp.Stop()

	// 由于任务处理成功，所有 offset 应该被处理
	// 注意：此测试依赖实际处理速度，可能不稳定，可改进为使用 WaitGroup
	close(processed)
	count := 0
	for range processed {
		count++
	}
	assert.Equal(t, 5, count, "all dispatched messages should be processed before stop")
}
