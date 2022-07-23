package index

import "time"

type (
	MemTable interface {
		Get(key []byte) (value *MemValue)
		Put(key []byte, value *MemValue) (replaced *MemValue)
		Delete(key []byte) (deleted *MemValue)
		Minimum() (key []byte, value *MemValue)
		Maximum() (key []byte, value *MemValue)
		Iterate() Iterator
		Size() int64
	}

	Iterator interface {
		HasNext() bool
		Next() (key []byte, value *MemValue)
	}
)

type (
	MemValue struct {
		FileID      int
		ValueOffset int64
		ValueSize   int
		ExprieAt    *time.Time
	}
)
