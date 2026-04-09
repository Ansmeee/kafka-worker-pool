package kafka_worker_pool

import (
	"context"
	"fmt"
)

type worker struct {
	ctx       context.Context
	eventType string
	queue     chan *eventTask
}

func (w *worker) run() {
	for {
		select {
		case <-w.ctx.Done():
			return
		case task := <-w.queue:
			w.execute(task)
		}
	}
}

func (w *worker) execute(task *eventTask) {
	defer func() {
		if err := recover(); err != nil {
			fmt.Printf("worker execute task panic: %v", err)
		}
	}()

	if err := task.handler(task.task); err != nil {
		fmt.Println("worker execute task fail: ", err.Error())
	}

	task.callback(task.task.msg.Offset, task.task.session)
}
