package peach

import (
	"time"

	"github.com/muyisensen/peach/index"
)

type (
	Options struct {
		DBPath string

		LogFileGCInterval    time.Duration
		LogFileSizeThreshold int64

		ArtOpt *index.AdaptiveRadixTreeOptions
	}

	optFunc func(opts *Options)
)

func DefaultOptions(dbPath string) *Options {
	return &Options{
		DBPath:               dbPath,
		LogFileGCInterval:    5 * time.Hour,
		LogFileSizeThreshold: 512 << 20,
		ArtOpt: &index.AdaptiveRadixTreeOptions{
			NodeLeafPoolSize: 512,
			Node4PoolSize:    256,
			Node16PoolSize:   128,
			Node48PoolSize:   64,
			Node256PoolSize:  32,
		},
	}
}

func LogFileGCInterval(interval time.Duration) optFunc {
	return func(opts *Options) {
		opts.LogFileGCInterval = interval
	}
}

func LogFileSizeThreshold(threshold int64) optFunc {
	return func(opts *Options) {
		opts.LogFileSizeThreshold = threshold
	}
}
