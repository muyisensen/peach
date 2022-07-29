package peach

import (
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestDB(t *testing.T) {
	os.RemoveAll("/tmp/test/kv")
	db, err := New(DefaultOptions("/tmp/test/kv"))
	assert.Nil(t, err)

	kvs := make([][]byte, 0, 1024)
	for i := 0; i < 1024; i++ {
		kv := []byte(uuid.New().String())
		assert.Nil(t, db.Put(kv, kv))
		kvs = append(kvs, kv)
	}
	assert.Nil(t, db.Sync())

	for _, kv := range kvs {
		value, err := db.Get(kv)
		assert.Nil(t, err)
		assert.True(t, reflect.DeepEqual(kv, value))
	}
	assert.Nil(t, db.Close())

	db2, err := New(DefaultOptions("/tmp/test/kv"))
	assert.Nil(t, err)

	for _, kv := range kvs {
		value, err := db2.Get(kv)
		assert.Nil(t, err)
		assert.True(t, reflect.DeepEqual(kv, value))
	}

	for _, kv := range kvs[:512] {
		assert.Nil(t, db2.Delete(kv))
	}
	assert.Equal(t, int64(512), db2.Size())

	for _, kv := range kvs[512:] {
		value, err := db2.Get(kv)
		assert.Nil(t, err)
		assert.True(t, reflect.DeepEqual(kv, value))
	}
	assert.Nil(t, db2.Close())

	db3, err := New(DefaultOptions("/tmp/test/kv"))
	assert.Nil(t, err)
	assert.Equal(t, int64(512), db3.Size())

	for _, kv := range kvs[:512] {
		_, err = db3.Get(kv)
		assert.Equal(t, ErrKeyNotFound, err)
	}

	for _, kv := range kvs[512:] {
		value, err := db3.Get(kv)
		assert.Nil(t, err)
		assert.True(t, reflect.DeepEqual(kv, value))
	}
	assert.Nil(t, db3.Close())
}

func TestSwitchActivedFile(t *testing.T) {
	os.RemoveAll("/tmp/test/kv")

	opts := DefaultOptions("/tmp/test/kv")
	opts.LogFileSizeThreshold = 10 << 10
	db, err := New(opts)
	assert.Nil(t, err)

	kvs := make([][]byte, 0, 10000)
	for i := 0; i < 10000; i++ {
		kv := []byte(uuid.New().String())
		assert.Nil(t, db.Put(kv, kv))
		kvs = append(kvs, kv)
	}
	assert.Equal(t, int64(10000), db.Size())
	assert.True(t, db.activedLogFile.FID() > 0)
	assert.True(t, len(db.archivedLogFile) > 0)

	for _, kv := range kvs {
		value, err := db.Get(kv)
		assert.Nil(t, err)
		assert.True(t, reflect.DeepEqual(kv, value))
	}
}

func TestLogFileGc(t *testing.T) {
	os.RemoveAll("/tmp/test/kv")

	opts := DefaultOptions("/tmp/test/kv")
	opts.LogFileSizeThreshold = 10 << 10
	db, err := New(opts)
	assert.Nil(t, err)

	kvs := make([][]byte, 0, 10000)
	for i := 0; i < 10000; i++ {
		kv := []byte(uuid.New().String())
		assert.Nil(t, db.Put(kv, kv))
		kvs = append(kvs, kv)
	}
	assert.Equal(t, int64(10000), db.Size())
	assert.True(t, db.activedLogFile.FID() > 0)
	assert.True(t, len(db.archivedLogFile) > 0)

	rand.Shuffle(len(kvs), func(i, j int) {
		kvs[i], kvs[j] = kvs[j], kvs[i]
	})
	for _, key := range kvs[:5000] {
		assert.Nil(t, db.Delete(key))
	}
	assert.Equal(t, int64(5000), db.Size())

	assert.Nil(t, db.startGc())
	assert.True(t, db.inGc)
	for {
		if !db.inGc {
			break
		}
		assert.Nil(t, db.doGc())
	}
	assert.Len(t, db.archivedLogFile, 0)

	infos, err := ioutil.ReadDir("/tmp/test/kv")
	assert.Nil(t, err)
	assert.Len(t, infos, 1)

	for _, kv := range kvs[5000:] {
		value, err := db.Get(kv)
		assert.Nil(t, err)
		assert.True(t, reflect.DeepEqual(value, kv))
	}

}
