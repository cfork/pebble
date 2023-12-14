package main

import (
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble"
)

const max = 1000 * 1000

var value = make([]byte, 1024)

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
		batch.Set(key, value, nil)
	}
	if err := batch.Commit(writeOpts); err != nil {
		log.Fatal(err)
	}

	go write(db)

	read(db)

}

func write(db *pebble.DB) {
	previ := 0
	ticker := time.NewTicker(5 * time.Second)
	for i := 0; ; i++ {
		select {
		case <-ticker.C:
			log.Println("write", i-previ, "ops/5 sec")
			previ = i
		default:
			key := []byte(strconv.Itoa(rand.Intn(max)))
			if err := db.Set(key, value, writeOpts); err != nil {
				log.Fatal(err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func read(db *pebble.DB) {
	ticker := time.NewTicker(5 * time.Second)
	previ := 0
	for i := 0; ; i++ {
		select {
		case <-ticker.C:
			log.Println("read", i-previ, "ops/5 sec")
			previ = i
		default:
			key := []byte(strconv.Itoa(rand.Intn(max)))
			_, closer, err := db.Get(key)
			if err != nil {
				log.Fatal(err)
			}
			if err := closer.Close(); err != nil {
				log.Fatal(err)
			}
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
