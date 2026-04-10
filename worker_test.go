package workerPool

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockTask struct {
	msg       *sarama.ConsumerMessage
	session   sarama.ConsumerGroupSession
	handler   func(*Task) error
	eventType string
}

func (m *mockTask) toTask() *Task {
	return &Task{
		Msg:       m.msg,
		session:   m.session,
		EventType: m.eventType,
	}
}

// mockSession 模拟 sarama.ConsumerGroupSession
type mockSession struct {
	sarama.ConsumerGroupSession
	markedOffsets map[string]map[int32]int64 // topic -> partition -> offset
	mu            sync.Mutex
}

func newMockSession() *mockSession {
	return &mockSession{
		markedOffsets: make(map[string]map[int32]int64),
	}
}

func (m *mockSession) MarkOffset(topic string, partition int32, offset int64, metadata string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.markedOffsets[topic]; !ok {
		m.markedOffsets[topic] = make(map[int32]int64)
	}
	m.markedOffsets[topic][partition] = offset
}

func (m *mockSession) GetMarkedOffset(topic string, partition int32) (int64, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if t, ok := m.markedOffsets[topic]; ok {
		off, exists := t[partition]
		return off, exists
	}
	return 0, false
}

// 创建测试用的 Worker
func newTestWorker(ctx context.Context, eventType string, queueSize int) *worker {
	return &worker{
		ctx:       ctx,
		eventType: eventType,
		queue:     make(chan *eventTask, queueSize),
	}
}

// 测试成功处理并提交 Offset
func TestWorker_Execute_Success(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := newTestWorker(ctx, "test", 1)
	session := newMockSession()
	msg := &sarama.ConsumerMessage{
		Topic:     "test-topic",
		Partition: 0,
		Offset:    100,
	}

	var callbackCalled bool
	var callbackErrCalled bool

	task := &eventTask{
		task: &Task{
			Msg:     msg,
			session: session,
		},
		handler: func(t *Task) error {
			return nil // 成功
		},
		callback: func(offset int64, sess sarama.ConsumerGroupSession) {
			callbackCalled = true
			sess.MarkOffset(msg.Topic, msg.Partition, offset+1, "")
		},
		callbackErr: func(t *Task) {
			callbackErrCalled = true
		},
	}

	// 执行
	w.execute(task)

	assert.True(t, callbackCalled, "success callback should be called")
	assert.False(t, callbackErrCalled, "error callback should not be called")

	markedOff, ok := session.GetMarkedOffset(msg.Topic, msg.Partition)
	assert.True(t, ok)
	assert.Equal(t, int64(101), markedOff)
}

func TestWorker_Execute_RetrySuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := newTestWorker(ctx, "test", 1)
	session := newMockSession()
	msg := &sarama.ConsumerMessage{Topic: "t", Partition: 0, Offset: 200}

	attempts := 0
	handler := func(t *Task) error {
		attempts++
		if attempts < 3 {
			return errors.New("temporary error")
		}
		return nil
	}

	var callbackCalled bool
	var callbackErrCalled bool

	task := &eventTask{
		task:    &Task{Msg: msg, session: session},
		handler: handler,
		callback: func(offset int64, sess sarama.ConsumerGroupSession) {
			callbackCalled = true
			sess.MarkOffset(msg.Topic, msg.Partition, offset+1, "")
		},
		callbackErr: func(t *Task) {
			callbackErrCalled = true
		},
	}

	start := time.Now()
	w.execute(task)
	elapsed := time.Since(start)

	assert.Equal(t, 3, attempts, "should have attempted 3 times")
	assert.True(t, callbackCalled)
	assert.False(t, callbackErrCalled)

	// 检查退避时间 (第一次无重试，第一次失败后重试1次等待100ms，第二次失败后重试等待200ms)
	// 总共等待约 100 + 200 = 300ms
	assert.GreaterOrEqual(t, elapsed, 300*time.Millisecond)
	assert.Less(t, elapsed, 500*time.Millisecond)

	markedOff, _ := session.GetMarkedOffset(msg.Topic, msg.Partition)
	assert.Equal(t, int64(201), markedOff)
}

func TestWorker_Execute_RetryExhausted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := newTestWorker(ctx, "test", 1)
	session := newMockSession()
	msg := &sarama.ConsumerMessage{Topic: "t", Partition: 0, Offset: 300}

	attempts := 0
	expectedErr := errors.New("persistent error")
	handler := func(t *Task) error {
		attempts++
		return expectedErr
	}

	var callbackCalled bool
	var callbackErrCalled bool
	var callbackErrTask *Task

	task := &eventTask{
		task:    &Task{Msg: msg, session: session},
		handler: handler,
		callback: func(offset int64, sess sarama.ConsumerGroupSession) {
			callbackCalled = true
		},
		callbackErr: func(t *Task) {
			callbackErrCalled = true
			callbackErrTask = t
		},
	}

	w.execute(task)

	// 1 次正常执行 + 5 次重试 = 6 次总尝试
	assert.Equal(t, 6, attempts)
	assert.True(t, callbackCalled, "success callback should not be called")
	assert.True(t, callbackErrCalled, "error callback should be called")
	assert.Equal(t, msg.Offset, callbackErrTask.Msg.Offset)

	// 确认没有提交 Offset
	_, ok := session.GetMarkedOffset(msg.Topic, msg.Partition)
	assert.False(t, ok, "offset should not be marked on failure")
}

func TestWorker_Execute_PanicRecovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	w := newTestWorker(ctx, "test", 1)
	session := newMockSession()
	msg := &sarama.ConsumerMessage{Topic: "t", Partition: 0, Offset: 400}

	handler := func(t *Task) error {
		panic("unexpected panic")
	}

	var callbackCalled bool
	var callbackErrCalled bool

	task := &eventTask{
		task:    &Task{Msg: msg, session: session},
		handler: handler,
		callback: func(offset int64, sess sarama.ConsumerGroupSession) {
			callbackCalled = true
		},
		callbackErr: func(t *Task) {
			callbackErrCalled = true
		},
	}

	// 不应 panic 到调用方
	require.NotPanics(t, func() {
		w.execute(task)
	})

	assert.True(t, callbackCalled)
	assert.True(t, callbackErrCalled)

	_, ok := session.GetMarkedOffset(msg.Topic, msg.Partition)
	assert.False(t, ok)
}

func TestWorker_RetryBackoff_ContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	w := newTestWorker(ctx, "test", 1)

	msg := &sarama.ConsumerMessage{Topic: "t", Partition: 0, Offset: 500}
	attempts := 0
	handler := func(t *Task) error {
		attempts++
		if attempts == 2 {
			cancel() // 第二次尝试时取消 context
		}
		return errors.New("error")
	}

	task := &eventTask{
		task:    &Task{Msg: msg},
		handler: handler,
	}

	err := w.retryBackoff(task)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 2, attempts) // 只重试了 1 次（首次执行在外部，retryBackoff 内执行第2次）
}
