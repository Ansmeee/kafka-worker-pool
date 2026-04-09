package workerPool

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
			fmt.Printf("worker execute task panic: %v\n", err)
			return
		}
	}()

	err := task.handler(task.task)
	if err != nil {
		fmt.Println("worker execute task fail: ", err.Error())
	}

	task.callback(task.task.Msg.Offset, task.task.session, err)
}
