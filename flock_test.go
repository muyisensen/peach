package peach

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFlock(t *testing.T) {
	fname := "/tmp/peach/LOCK"
	os.Remove(fname)

	flock := NewFlock(fname)
	assert.Nil(t, flock.TryLock())

	otherFlock := NewFlock(fname)
	assert.NotNil(t, otherFlock.TryLock())

	assert.Nil(t, flock.ULock())
	assert.Nil(t, otherFlock.ULock())
}
