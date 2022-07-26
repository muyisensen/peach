package peach

import (
	"io"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/muyisensen/peach/index"
	"github.com/stretchr/testify/assert"
)

func TestLogFile(t *testing.T) {
	os.Remove("/tmp/log.0")

	lf, err := NewLogFile("/tmp", 0)
	assert.Nil(t, err)

	size, err := lf.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), size)

	offset := int64(0)
	les := make([]*LogEntry, 0, 1024)
	vals := make([]*index.MemValue, 0, 1024)
	for i := 0; i < 1024; i++ {
		kv := []byte(uuid.New().String())

		logEntryType, value := Normal, kv
		if i%2 == 0 {
			logEntryType, value = Delete, []byte{}
		}

		le := &LogEntry{
			Type:      logEntryType,
			Timestamp: time.Now().Unix(),
			Key:       kv,
			Value:     value,
		}
		les = append(les, le)

		n, err := lf.Write(offset, le)
		assert.Nil(t, err)
		vals = append(vals, &index.MemValue{
			Offset: offset,
			Size:   n,
		})

		offset += int64(n)
	}

	size, err = lf.Size()
	assert.Nil(t, err)
	assert.True(t, size > 0)

	for index, item := range vals {
		val := item
		le, err := lf.Read(val.Offset, val.Size)
		assert.Nil(t, err)
		assert.True(t, reflect.DeepEqual(le, les[index]))
	}

	offset = int64(0)
	les2 := make([]*LogEntry, 0, 1024)
	for {
		le, n, err := lf.Load(offset)
		switch err {
		case nil:
		case io.EOF:
			assert.True(t, reflect.DeepEqual(les, les2))
			return
		default:
			t.Fatalf("err: %v", err.Error())
		}
		les2 = append(les2, le)
		offset += int64(n)
	}
}
