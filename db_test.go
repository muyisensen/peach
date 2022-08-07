package peach

import (
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/muyisensen/peach/utils"
	"github.com/stretchr/testify/assert"
)

func TestDB(t *testing.T) {
	dbPath := "/tmp/peach"
	os.RemoveAll(dbPath)
	db, err := New(DefaultOptions(dbPath))
	assert.Nil(t, err)

	kvs := make([][]byte, 0, 1024)
	for i := 0; i < 1024; i++ {
		kv := utils.RandBytes(36)
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

	db2, err := New(DefaultOptions(dbPath))
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

	db3, err := New(DefaultOptions(dbPath))
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
	dbPath := "/tmp/peach"
	os.RemoveAll(dbPath)

	opts := DefaultOptions(dbPath)
	opts.LogFileSizeThreshold = 10 << 10
	db, err := New(opts)
	assert.Nil(t, err)

	kvs := make([][]byte, 0, 10000)
	for i := 0; i < 10000; i++ {
		kv := utils.RandBytes(36)
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
	dbPath := "/tmp/peach"
	os.RemoveAll(dbPath)
	opts := DefaultOptions(dbPath)
	opts.LogFileSizeThreshold = 10 << 10
	db, err := New(opts)
	assert.Nil(t, err)

	total, i := 10000, 0
	kvs, exist := make([][]byte, 0, total), make(map[string]struct{})
	for i < total {
		kv := utils.RandBytes(36)
		if _, ok := exist[string(kv)]; ok {
			continue
		}
		exist[string(kv)] = struct{}{}
		i++

		kvs = append(kvs, kv)
		assert.Nil(t, db.Put(kv, kv))
	}
	assert.Equal(t, int64(total), db.Size())
	assert.True(t, db.activedLogFile.FID() > 0)
	assert.True(t, len(db.archivedLogFile) > 0)

	rand.Shuffle(len(kvs), func(i, j int) {
		kvs[i], kvs[j] = kvs[j], kvs[i]
	})
	for _, key := range kvs[:total/2] {
		assert.Nil(t, db.Delete(key))
	}

	for _, key := range kvs[total/2:] {
		value, err := db.Get(key)
		assert.Nil(t, err)
		assert.True(t, reflect.DeepEqual(value, key))
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

	infos, err := ioutil.ReadDir(dbPath)
	assert.Nil(t, err)
	count := 0
	for i := 0; i < len(infos); i++ {
		if strings.HasPrefix(infos[i].Name(), LogFileNamePrefix) {
			count++
		}
	}
	assert.Equal(t, count, 1)

	for _, kv := range kvs[5000:] {
		value, err := db.Get(kv)
		assert.Nil(t, err)
		assert.True(t, reflect.DeepEqual(value, kv))
	}
}

func BenchmarkSet(b *testing.B) {
	dbPath := "/tmp/peach"
	os.RemoveAll(dbPath)
	db, err := New(DefaultOptions(dbPath))
	assert.Nil(b, err)
	defer db.Close()

	kvs := make([][]byte, 0, b.N)
	for i := 0; i < b.N; i++ {
		kvs = append(kvs, utils.RandBytes(36))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		kv := kvs[i]
		assert.Nil(b, db.Put(kv, kv))
	}
}

func BenchmarkGet(b *testing.B) {
	dbPath := "/tmp/peach/test"
	os.RemoveAll(dbPath)
	db, err := New(DefaultOptions(dbPath))
	assert.Nil(b, err)
	defer db.Close()

	kvs := make([][]byte, 0, b.N)
	for i := 0; i < b.N; i++ {
		kv := utils.RandBytes(36)
		kvs = append(kvs, kv)
		assert.Nil(b, db.Put(kv, kv))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		kv := kvs[i]
		value, err := db.Get(kv)
		assert.Nil(b, err)
		assert.True(b, reflect.DeepEqual(kv, value))
	}
}

func BenchmarkDelete(b *testing.B) {
	dbPath := "/tmp/peach"
	os.RemoveAll(dbPath)
	db, err := New(DefaultOptions(dbPath))
	assert.Nil(b, err)
	defer db.Close()

	kvs := make([][]byte, 0, b.N)
	for i := 0; i < b.N; i++ {
		kv := utils.RandBytes(36)
		kvs = append(kvs, kv)
		assert.Nil(b, db.Put(kv, kv))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		kv := kvs[i]
		assert.Nil(b, db.Delete(kv))
	}
}
