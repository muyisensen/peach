package peach

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

type (
	LogEntryType uint8

	LogEntry struct {
		Type      LogEntryType
		Timestamp int64
		Key       []byte
		Value     []byte
	}
)

const (
	MaxLogEntryHeaderSize = 25

	Normal LogEntryType = iota + 1
	Delete
	ExpiredAt
)

var (
	ErrRawSizeTooShort  = errors.New("raw data size too short to decode")
	ErrCheckSumNotMatch = errors.New("crc check sum not match")
)

func Encode(le *LogEntry) []byte {
	header := make([]byte, MaxLogEntryHeaderSize)

	index := 5
	index += binary.PutUvarint(header[index:], uint64(len(le.Key)))
	index += binary.PutUvarint(header[index:], uint64(len(le.Value)))
	index += binary.PutUvarint(header[index:], uint64(le.Timestamp))
	header[4] = byte(le.Type)

	size := index + len(le.Key) + len(le.Value)
	buf := make([]byte, size)
	copy(buf[:index], header)
	copy(buf[index:index+len(le.Key)], le.Key)
	copy(buf[index+len(le.Key):], le.Value)

	crc := crc32.ChecksumIEEE(buf[4:])
	binary.LittleEndian.PutUint32(buf[:4], crc)

	return buf
}

func Decode(raw []byte) (*LogEntry, error) {
	if len(raw) < 4 {
		return nil, ErrRawSizeTooShort
	}

	crc := binary.LittleEndian.Uint32(raw[:4])
	reCrc := crc32.ChecksumIEEE(raw[4:])
	if crc != reCrc {
		return nil, ErrCheckSumNotMatch
	}

	index := 5
	keySize, n := binary.Uvarint(raw[index:])
	index += n

	valueSize, n := binary.Uvarint(raw[index:])
	index += n

	timestamp, n := binary.Uvarint(raw[index:])
	index += n

	return &LogEntry{
		Type:      LogEntryType(raw[4]),
		Timestamp: int64(timestamp),
		Key:       raw[index : index+int(keySize)],
		Value:     raw[index+int(keySize) : index+int(keySize)+int(valueSize)],
	}, nil
}
