package workerPool

import (
	"context"
	"fmt"
	"time"
)

const maxRetryTimes = 5

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
	var err error

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
			fmt.Println("worker task handler panic", err.Error())
		}

		if err != nil {
			task.callbackErr(task.task)
		}

		task.callback(task.task.Msg.Offset, task.task.session)
	}()

	err = task.handler(task.task)
	if err == nil {
		return
	}

	err = w.retryBackoff(task)
}

func (w *worker) retryBackoff(task *eventTask) error {
	for i := 0; i < maxRetryTimes; i++ {
		select {
		case <-w.ctx.Done():
			return w.ctx.Err()
		default:
		}

		backOff := time.Duration(100*(1<<i)) * time.Millisecond
		fmt.Printf("worker task retry %d after %d ms  \n", i, backOff.Milliseconds())

		time.Sleep(backOff)
		if err := task.handler(task.task); err == nil {
			return nil
		}
	}

	return fmt.Errorf("task handler error over max retry times: %d", maxRetryTimes)
}
