package peach

import (
	"errors"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/muyisensen/peach/index"
	"github.com/muyisensen/peach/index/art"
	"github.com/muyisensen/peach/utils"
)

var (
	ErrLogFileNotExist = errors.New("log file not exist")
	ErrKeyNotFound     = errors.New("key not found")
)

type (
	DB struct {
		mu              sync.RWMutex
		size            int64
		opts            *Options
		index0          index.MemTable
		activedLogFile  *LogFile
		offset          int64
		archivedLogFile map[int]*LogFile
		index1          index.MemTable
		inGc            bool
		lastGCTime      time.Time
	}
)

func New(opts *Options) (*DB, error) {
	if !utils.Exist(opts.DBPath) {
		if err := os.MkdirAll(opts.DBPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	db := &DB{
		opts:            opts,
		index0:          art.NewAdaptiveRadixTree(opts.ArtOpt),
		archivedLogFile: make(map[int]*LogFile),
	}

	if err := db.reload(); err != nil {
		return nil, err
	}

	go db.eventHandle()

	return db, nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	memValue := db.index0.Get(key)
	if memValue == nil && db.index1 != nil {
		memValue = db.index1.Get(key)
	}

	if memValue == nil {
		return nil, ErrKeyNotFound
	}

	var logFile *LogFile
	if lf, ok := db.archivedLogFile[memValue.FileID]; ok {
		logFile = lf
	}

	if db.activedLogFile.FID() == memValue.FileID {
		logFile = db.activedLogFile
	}

	if logFile == nil {
		return nil, ErrLogFileNotExist
	}

	le, err := logFile.Read(memValue.Offset, memValue.Size)
	if err != nil {
		return nil, err
	}

	return le.Value, nil
}

func (db *DB) Put(key, value []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	size, err := db.activedLogFile.Write(db.offset, &LogEntry{
		Type:      Normal,
		Timestamp: time.Now().Unix(),
		Key:       key,
		Value:     value,
	})
	if err != nil {
		return err
	}

	memValue := &index.MemValue{
		FileID: db.activedLogFile.FID(),
		Offset: db.offset,
		Size:   size,
	}
	db.offset += int64(size)

	var replaced *index.MemValue
	if db.inGc && db.index1 != nil {
		replaced = db.index1.Put(key, memValue)
	} else {
		replaced = db.index0.Put(key, memValue)
	}

	if replaced == nil {
		db.size++
	}

	if err := db.doGc(); err != nil {
		log.Printf("doGc fail, err msg: %v", err.Error())
	}

	if fileSize, err := db.activedLogFile.Size(); err != nil {
		log.Printf("call LogFile.Size() fail, err msg: %v", err.Error())
	} else if fileSize > db.opts.LogFileSizeThreshold && !db.inGc {
		if err := db.switchActivedLogFile(); err != nil {
			log.Printf("call switchActivedLogFile fail, err msg: %v", err.Error())
		}
	}

	return nil
}

func (db *DB) Delete(key []byte) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	deleted := db.index0.Get(key)
	if deleted == nil && db.index1 != nil {
		deleted = db.index1.Get(key)
	}

	if deleted == nil {
		return nil
	}

	size, err := db.activedLogFile.Write(db.offset, &LogEntry{
		Type:      Delete,
		Timestamp: time.Now().Unix(),
		Key:       key,
		Value:     []byte{},
	})
	if err != nil {
		return err
	}
	db.offset += int64(size)

	if db.inGc && db.index1 != nil {
		db.index1.Delete(key)
	} else {
		db.index0.Delete(key)
	}
	db.size--

	if err := db.doGc(); err != nil {
		log.Printf("doGc fail, err msg: %v", err.Error())
	}

	if fileSize, err := db.activedLogFile.Size(); err != nil {
		log.Printf("call LogFile.Size() fail, err msg: %v", err.Error())
	} else if fileSize > db.opts.LogFileSizeThreshold && !db.inGc {
		if err := db.switchActivedLogFile(); err != nil {
			log.Printf("call switchActivedLogFile fail, err msg: %v", err.Error())
		}
	}

	return nil
}

func (db *DB) Sync() error {
	if err := db.activedLogFile.Sync(); err != nil {
		return nil
	}
	return nil
}

func (db *DB) Close() error {
	if err := db.Sync(); err != nil {
		return err
	}

	if err := db.activedLogFile.Close(); err != nil {
		return err
	}

	for _, item := range db.archivedLogFile {
		logFile := item
		if err := logFile.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB) Size() int64 {
	return db.size
}

func (db *DB) reload() error {
	infos, err := ioutil.ReadDir(db.opts.DBPath)
	if err != nil {
		return err
	}

	fids := make([]int, 0, len(infos))
	for _, info := range infos {
		if !strings.HasPrefix(info.Name(), LogFileNamePrefix) {
			continue
		}

		items := strings.Split(info.Name(), ".")
		if len(items) < 2 {
			continue
		}

		fid, err := strconv.Atoi(items[1])
		if err != nil {
			return err
		}
		fids = append(fids, fid)
	}
	sort.Ints(fids)

	for i, fid := range fids {
		logFile, err := NewLogFile(db.opts.DBPath, fid)
		if err != nil {
			return err
		}

		offset, err := db.reloadIndex(logFile)
		if err != nil {
			return err
		}

		if i == len(fids)-1 {
			db.offset = offset
			db.activedLogFile = logFile
		} else {
			db.archivedLogFile[fid] = logFile
		}
	}

	if db.activedLogFile == nil {
		logFile, err := NewLogFile(db.opts.DBPath, 0)
		if err != nil {
			return nil
		}
		db.activedLogFile = logFile
	}

	return nil
}

func (db *DB) reloadIndex(lf *LogFile) (int64, error) {
	offset := int64(0)
	for {
		le, size, err := lf.Load(offset)
		switch err {
		case nil:
		case io.EOF:
			return offset, nil
		default:
			return 0, err
		}

		if le.Type == Delete {
			if deleted := db.index0.Delete(le.Key); deleted != nil {
				db.size--
			}
			offset += int64(size)
			continue
		}

		var expiredAt *int64
		if le.Type == ExpiredAt {
			if time.Now().Unix() < le.Timestamp {
				expiredAt = &le.Timestamp
			} else {
				offset += int64(size)
				continue
			}
		}

		db.index0.Put(le.Key, &index.MemValue{
			FileID:    lf.fid,
			Offset:    offset,
			Size:      size,
			ExpiredAt: expiredAt,
		})
		offset += int64(size)
		db.size++
	}
}

func (db *DB) eventHandle() {
	logFileGcTicker := time.NewTicker(db.opts.LogFileGCInterval)
	gcTicker := time.NewTicker(time.Second)
	for {
		select {
		case <-logFileGcTicker.C:
			if db.inGc {
				continue
			}
			if err := db.startGc(); err != nil {
				log.Printf("start gc fail, err msg: %v", err.Error())
			}
		case <-gcTicker.C:
			if err := db.gc(); err != nil {
				log.Printf("gc fail, err msg: %v", err.Error())
			}
		default:
			time.Sleep(5 * time.Second)
		}
	}
}

func (db *DB) startGc() error {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.inGc = true
	db.index1 = art.NewAdaptiveRadixTree(db.opts.ArtOpt)
	db.lastGCTime = time.Now()

	return db.switchActivedLogFile()
}

func (db *DB) gc() error {
	now := time.Now()
	if !db.inGc || now.Before(db.lastGCTime.Add(5*time.Second)) {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	timeout := time.NewTicker(500 * time.Millisecond)
	for {
		select {
		case <-timeout.C:
			return nil
		default:
			if err := db.doGc(); err != nil {
				return err
			}
		}
	}
}

func (db *DB) doGc() error {
	if !db.inGc {
		return nil
	}

	key, value := db.index0.Minimum()
	if len(key) == 0 || value == nil {
		db.index0 = db.index1
		db.index1 = nil
		db.inGc = false
		return db.removeArchivedLogFile()
	}

	if value.FileID == db.activedLogFile.FID() {
		return nil
	}

	now := time.Now().Unix()
	if value.ExpiredAt != nil && *value.ExpiredAt < now {
		db.index0.Delete(key)
		db.lastGCTime = time.Now()
		return nil
	}

	logFile, ok := db.archivedLogFile[value.FileID]
	if !ok {
		return ErrLogFileNotExist
	}

	le, err := logFile.Read(value.Offset, value.Size)
	switch err {
	case nil:
	case io.EOF:
		return nil
	default:
		return err
	}

	size, err := db.activedLogFile.Write(db.offset, le)
	if err != nil {
		return err
	}

	value.FileID = db.activedLogFile.FID()
	value.Offset = db.offset
	db.index1.Put(key, value)
	db.index0.Delete(key)
	db.lastGCTime = time.Now()
	db.offset += int64(size)

	return nil
}

func (db *DB) switchActivedLogFile() error {
	current := db.activedLogFile
	currentFid := current.FID()
	logFile, err := NewLogFile(db.opts.DBPath, currentFid+1)
	if err != nil {
		return err
	}

	db.archivedLogFile[currentFid] = db.activedLogFile
	db.activedLogFile = logFile
	db.offset = 0
	return current.Sync()
}

func (db *DB) removeArchivedLogFile() error {
	fids := make([]int, 0, len(db.archivedLogFile))
	for fid := range db.archivedLogFile {
		fids = append(fids, fid)
	}
	sort.Ints(fids)

	for _, fid := range fids {
		if lf, ok := db.archivedLogFile[fid]; ok {
			if err := os.Remove(lf.Path()); err != nil {
				return err
			}
			delete(db.archivedLogFile, fid)
		}

	}

	return nil
}
