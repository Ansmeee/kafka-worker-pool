package workerPool

import (
	"context"
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
	task.callback(task.task.Msg.Offset, task.task.session, task.handler(task.task))
}
