package main

import (
	"log"
	"math/rand"
	"reflect"
	"runtime/internal/atomic"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble"
)

const (
	max                 = 1000 * 1000
	valueSize           = 1024
	printFrequencyInSec = 5
)

var (
	writeCounter atomic.Int64
	readCounter  atomic.Int64
)

var value = make([]byte, valueSize)

func init() {
	for i := 0; i < len(value); i++ {
		value[i] = 'a'
	}
}

var writeOpts = &pebble.WriteOptions{
	Sync: false,
}

func main() {
	db := newBenchDB("tmp")
	max := 1000 * 1000

	// init
	batch := db.NewBatch()
	for i := 0; i < max; i++ {
		key := []byte(strconv.Itoa(i))
		newValue := getValue(i)
		if err := batch.Set(key, newValue, nil); err != nil {
			log.Fatal(err)
		}
	}
	if err := batch.Commit(writeOpts); err != nil {
		log.Fatal(err)
	}
	db.Flush()

	go metrics()
	go write(db)

	for i := 0; i < 5; i++ {
		go read(db)
	}

	time.Sleep(time.Hour)
}

func metrics() {
	ticker := time.NewTicker(printFrequencyInSec * time.Second)
	for range ticker.C {
		w := writeCounter.Swap(0)
		r := readCounter.Swap(0)
		log.Printf("write: %d ops/s, read: %d ops/s", w/printFrequencyInSec, r/printFrequencyInSec)
	}
}

func write(db *pebble.DB) {
	for i := 0; ; i++ {
		writeCounter.Add(1)
		n := rand.Intn(max)
		key := []byte(strconv.Itoa(n))
		newValue := getValue(n)
		if err := db.Set(key, newValue, writeOpts); err != nil {
			log.Fatal(err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func read(db *pebble.DB) {
	for i := 0; ; i++ {
		readCounter.Add(1)
		n := rand.Intn(max)
		key := []byte(strconv.Itoa(n))
		val, closer, err := db.Get(key)
		if err != nil {
			log.Fatal(err)
		}
		newValue := getValue(n)
		if !reflect.DeepEqual(val, newValue) {
			log.Fatalf("not equal: %d\n%s\n%s", i, string(val), string(newValue))
		}
		if err := closer.Close(); err != nil {
			log.Fatal(err)
		}
	}
}

func newBenchDB(dir string) *pebble.DB {
	opts := &pebble.Options{
		DisableWAL: true,
	}

	p, err := pebble.Open(dir, opts)
	if err != nil {
		log.Fatal(err)
	}
	return p
}

func getValue(val int) []byte {
	ret := make([]byte, valueSize)
	copy(ret, value)
	b := []byte(strconv.Itoa(val))
	copy(ret, b)
	return ret
}
