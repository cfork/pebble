package main

import (
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/bloom"
	"github.com/cockroachdb/pebble/objstorage/remote"
	"github.com/cockroachdb/pebble/vfs"
)

func runmy() {
	db := newBenchDB("tmp")
	max := 1000 * 1000
	writeOpts := &pebble.WriteOptions{
		Sync: false,
	}

	// init
	batch := db.NewBatch()
	for i := 0; i < max; i++ {
		key := []byte(strconv.Itoa(i))
		batch.Set(key, value, nil)
	}
	batch.Commit(writeOpts)

	// write

	go func() {
		previ := 0
		ticker := time.NewTicker(5 * time.Second)
		for i := 0; ; i++ {
			select {
			case <-ticker.C:
				log.Println("write", i-previ, "ops/5 sec")
				previ = i
			default:
				batch := db.NewBatch()
				key := []byte(strconv.Itoa(rand.Intn(max)))
				batch.Set(key, value, nil)
				batch.Commit(writeOpts)
				time.Sleep(time.Millisecond)
			}
		}
	}()

	ticker := time.NewTicker(5 * time.Second)
	previ := 0
	for i := 0; ; i++ {
		select {
		case <-ticker.C:
			log.Println("read", i-previ, "ops/5 sec")
			previ = i
		default:
			key := []byte(strconv.Itoa(rand.Intn(max)))
			iter := db.NewIter(nil)
			iter.SeekGE(key)
			if iter.Valid() {
				_ = iter.Key()
				_ = iter.Value()
			}
			iter.Close()
		}
	}

}

func newBenchDB(dir string) DB {
	cache := pebble.NewCache(cacheSize)
	defer cache.Unref()
	opts := &pebble.Options{
		Cache:                       cache,
		Comparer:                    mvccComparer,
		DisableWAL:                  true,
		FormatMajorVersion:          pebble.FormatNewest,
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       1000,
		LBaseMaxBytes:               64 << 20, // 64 MB
		Levels:                      make([]pebble.LevelOptions, 7),
		MaxOpenFiles:                16384,
		MemTableSize:                64 << 20,
		MemTableStopWritesThreshold: 4,
		Merger: &pebble.Merger{
			Name: "cockroach_merge_operator",
		},
		MaxConcurrentCompactions: func() int {
			return 3
		},
	}

	for i := 0; i < len(opts.Levels); i++ {
		l := &opts.Levels[i]
		l.BlockSize = 32 << 10       // 32 KB
		l.IndexBlockSize = 256 << 10 // 256 KB
		l.FilterPolicy = bloom.FilterPolicy(10)
		l.FilterType = pebble.TableFilter
		if i > 0 {
			l.TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}
		l.EnsureDefaults()
	}
	opts.Levels[6].FilterPolicy = nil
	opts.FlushSplitBytes = opts.Levels[0].TargetFileSize

	opts.EnsureDefaults()

	if verbose {
		lel := pebble.MakeLoggingEventListener(nil)
		opts.EventListener = &lel
		opts.EventListener.TableDeleted = nil
		opts.EventListener.TableIngested = nil
		opts.EventListener.WALCreated = nil
		opts.EventListener.WALDeleted = nil
	}

	if pathToLocalSharedStorage != "" {
		opts.Experimental.RemoteStorage = remote.MakeSimpleFactory(map[remote.Locator]remote.Storage{
			// Store all shared objects on local disk, for convenience.
			"": remote.NewLocalFS(pathToLocalSharedStorage, vfs.Default),
		})
		opts.Experimental.CreateOnShared = remote.CreateOnSharedAll
		if secondaryCacheSize != 0 {
			opts.Experimental.SecondaryCacheSizeBytes = secondaryCacheSize
		}
	}

	p, err := pebble.Open(dir, opts)
	if err != nil {
		log.Fatal(err)
	}
	if pathToLocalSharedStorage != "" {
		if err := p.SetCreatorID(1); err != nil {
			log.Fatal(err)
		}
	}
	return pebbleDB{
		d:       p,
		ballast: make([]byte, 1<<30),
	}
}
