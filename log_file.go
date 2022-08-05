package peach

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
)

const (
	LogFileNamePrefix = "log."
)

type (
	LogFile struct {
		fid  int
		path string
		file *os.File
		size int64
	}
)

func NewLogFile(dirPath string, fid int) (*LogFile, error) {
	fileName := fmt.Sprintf("%s%d", LogFileNamePrefix, fid)
	path := filepath.Join(dirPath, fileName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return &LogFile{file: file, fid: fid, path: path}, nil
}

func (f *LogFile) Read(offset int64, size int) (*LogEntry, error) {
	buf := make([]byte, size)
	_, err := f.file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}

	return Decode(buf)
}

func (f *LogFile) Load(offset int64) (*LogEntry, int, error) {
	header := make([]byte, MaxLogEntryHeaderSize)
	_, err := f.file.ReadAt(header, offset)
	if err != nil {
		return nil, 0, err
	}

	index := 5
	keySize, n := binary.Uvarint(header[index:])
	index += n

	valueSize, n := binary.Uvarint(header[index:])
	index += n

	_, n = binary.Uvarint(header[index:])
	index += n

	kvBuf := make([]byte, keySize+valueSize)
	_, err = f.file.ReadAt(kvBuf, offset+int64(index))
	if err != nil {
		return nil, 0, err
	}

	buf := make([]byte, 0, index+int(keySize)+int(valueSize))
	buf = append(buf, header[:index]...)
	buf = append(buf, kvBuf...)

	le, err := Decode(buf)
	if err != nil {
		return nil, 0, err
	}

	return le, len(buf), nil

}

func (f *LogFile) Write(offset int64, le *LogEntry) (int, error) {
	buf := Encode(le)

	n, err := f.file.WriteAt(buf, offset)
	if err != nil {
		return 0, err
	}
	f.size += int64(n)

	return n, nil
}

func (f *LogFile) Sync() error {
	return f.file.Sync()
}

func (f *LogFile) Close() error {
	return f.file.Close()
}

func (f *LogFile) FID() int {
	return f.fid
}

func (f *LogFile) Path() string {
	return f.path
}

func (f *LogFile) Size() (int64, error) {
	if f.size > 0 {
		return f.size, nil
	}

	stat, err := f.file.Stat()
	if err != nil {
		return 0, err
	}
	f.size = stat.Size()

	return f.size, nil
}
