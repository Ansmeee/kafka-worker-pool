package kafka_worker_pool

import (
	"context"
	"sync"

	"github.com/IBM/sarama"
)

type partition struct {
	ctx           context.Context
	partition     int32
	topic         string
	taskQueue     chan *Task
	dispatcher    *dispatcher
	offsetTracker *offsetTracker
}

type dispatcher struct {
	mu          sync.Mutex
	ctx         context.Context
	maxTaskSize int64
	workers     map[string]*worker
	handler     func(task *Task) error
}

type offsetTracker struct {
	mu sync.Mutex

	committedOffset int64
	completed       map[int64]bool
	initialized     bool
}

func (p *partition) consume() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case task := <-p.taskQueue:
			p.dispatcher.dispatch(task, p.onCompletion)
		}
	}
}

func (p *partition) onCompletion(offset int64, session sarama.ConsumerGroupSession) {
	newOffset := p.offsetTracker.markDone(offset)
	session.MarkOffset(p.topic, p.partition, newOffset+1, "")
}

func (p *partition) generateTask(msg *sarama.ConsumerMessage, session sarama.ConsumerGroupSession) *Task {
	eventType := "default"
	for _, header := range msg.Headers {
		if string(header.Key) == "x-event-type" {
			eventType = string(header.Value)
			break
		}
	}

	return &Task{
		msg:       msg,
		session:   session,
		eventType: eventType,
	}
}

func (ot *offsetTracker) markDone(offset int64) int64 {
	ot.mu.Lock()
	defer ot.mu.Unlock()

	if !ot.initialized {
		ot.initialized = true
		ot.committedOffset = offset - 1
	}

	ot.completed[offset] = true
	next := ot.committedOffset + 1

	for {
		if ot.completed[next] {
			delete(ot.completed, next)
			ot.committedOffset = next
			next++
			continue
		}

		break
	}

	return ot.committedOffset
}

func (dp *dispatcher) dispatch(task *Task, callback func(offset int64, session sarama.ConsumerGroupSession)) {
	dp.mu.Lock()
	w, ok := dp.workers[task.eventType]
	if !ok {
		w = &worker{
			ctx:       dp.ctx,
			eventType: task.eventType,
			queue:     make(chan *eventTask, dp.maxTaskSize),
		}

		dp.workers[task.eventType] = w
		go w.run()
	}
	defer dp.mu.Unlock()

	w.queue <- &eventTask{
		task:     task,
		handler:  dp.handler,
		callback: callback,
	}
}
