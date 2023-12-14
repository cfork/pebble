// Copyright 2019 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package main

import (
	"log"
	"math/rand"
	"strconv"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/spf13/cobra"
)

var myCmd = &cobra.Command{
	Use:   "my <dir>",
	Short: "run customizable MY benchmark",
	Long: `
`,
	Args: cobra.ExactArgs(1),
	RunE: runMY,
}

const max = 1000 * 1000

var value = make([]byte, 1024)

func init() {
	for i := range value {
		value[i] = 'x'
	}
}

type MY struct {
	db        DB
	writeOpts *pebble.WriteOptions
}

func NewMY() *MY {
	dir := "."
	db := newPebbleDB(dir)
	return &MY{
		db: db,
		writeOpts: &pebble.WriteOptions{
			Sync: false,
		},
	}
}

func runMY(cmd *cobra.Command, args []string) error {
	m := NewMY()
	m.myInit()
	go m.myWrite()
	m.myRead()
	return nil
}

func (m *MY) myInit() {
	b := m.db.NewBatch()
	for i := 0; i < max; i++ {
		key := []byte(strconv.Itoa(i))
		b.Set(key, value, nil)
	}
	b.Commit(m.writeOpts)
}

func (m *MY) myWrite() {
	for i := 0; ; i++ {
		if i%10*1000 == 0 {
			log.Printf("read %d\n", i)
		}
		b := m.db.NewBatch()
		i := rand.Intn(max)
		key := []byte(strconv.Itoa(i))
		b.Set(key, value, nil)
		b.Commit(m.writeOpts)
		time.Sleep(10 * time.Microsecond)
	}
}

func (m *MY) myRead() {
	for i := 0; ; i++ {
		if i%10*1000 == 0 {
			log.Printf("read %d\n", i)
		}
		n := rand.Intn(max)
		key := []byte(strconv.Itoa(n))
		iter := m.db.NewIter(nil)
		iter.SeekGE(key)
		if !iter.Valid() {
			_ = iter.Key()
			_ = iter.Value()
		}
	}
}
