package peach

import (
	"encoding/binary"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLogEntry(t *testing.T) {
	le := &LogEntry{
		Type:      Normal,
		Timestamp: time.Now().Unix(),
		Key:       []byte("hello"),
		Value:     []byte("world"),
	}

	raw := Encode(le)
	assert.True(t, len(raw) > 5)
	assert.Equal(t, byte(Normal), raw[4])
	index := 5

	keySize, n := binary.Uvarint(raw[index:])
	assert.Equal(t, uint64(5), keySize)
	index += n

	valueSize, n := binary.Uvarint(raw[index:])
	assert.Equal(t, uint64(5), valueSize)
	index += n

	timestamp, n := binary.Uvarint(raw[index:])
	assert.Equal(t, uint64(le.Timestamp), timestamp)
	index += n

	key := raw[index : index+int(keySize)]
	assert.True(t, reflect.DeepEqual(key, le.Key))
	index += int(keySize)

	value := raw[index:]
	assert.True(t, reflect.DeepEqual(value, le.Value))

	decodeLe, err := Decode(raw)
	assert.Nil(t, err)
	assert.True(t, reflect.DeepEqual(le, decodeLe))

	le = &LogEntry{
		Type:      Delete,
		Timestamp: time.Now().Unix(),
		Key:       []byte("hello"),
		Value:     []byte{},
	}

	deletedLe, err := Decode(Encode(le))
	assert.Nil(t, err)
	assert.True(t, reflect.DeepEqual(le, deletedLe))
}
