package workerPool

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOffsetTracker_Sequential(t *testing.T) {
	ot := &offsetTracker{completed: make(map[int64]bool)}
	ot.init(10)

	// 顺序完成
	assert.Equal(t, int64(10), ot.markDone(10))
	assert.Equal(t, int64(11), ot.markDone(11))
	assert.Equal(t, int64(12), ot.markDone(12))
}

func TestOffsetTracker_OutOfOrder(t *testing.T) {
	ot := &offsetTracker{completed: make(map[int64]bool)}
	ot.init(10)

	// 乱序完成
	ot.markDone(12)
	assert.Equal(t, int64(9), ot.committedOffset) // 未推进

	ot.markDone(11)
	assert.Equal(t, int64(9), ot.committedOffset) // 仍未推进，因为 10 未完成

	newOffset := ot.markDone(10)
	assert.Equal(t, int64(12), newOffset) // 连续推进到 12
	assert.Equal(t, int64(12), ot.committedOffset)

	// 确认 map 中已清理
	_, ok10 := ot.completed[10]
	_, ok11 := ot.completed[11]
	_, ok12 := ot.completed[12]
	assert.False(t, ok10)
	assert.False(t, ok11)
	assert.False(t, ok12)
}

func TestOffsetTracker_DuplicateMark(t *testing.T) {
	ot := &offsetTracker{completed: make(map[int64]bool)}
	ot.init(10)

	ot.markDone(10)
	ot.markDone(10) // 重复标记
	assert.Equal(t, int64(10), ot.committedOffset)
}

func TestOffsetTracker_InitOnlyOnce(t *testing.T) {
	ot := &offsetTracker{completed: make(map[int64]bool)}
	ot.init(100)
	ot.init(200) // 不应改变状态
	assert.Equal(t, int64(99), ot.committedOffset)
}
