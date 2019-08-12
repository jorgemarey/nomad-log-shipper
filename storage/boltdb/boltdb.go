package boltdb

import (
	"encoding/binary"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
	"github.com/jorgemarey/nomad-log-shipper/storage"
)

type boltStore struct {
	db *bolt.DB

	cachedFK map[string][]byte
	cachedOK map[string][]byte
}

func NewBoltDBStore() storage.Store {
	return &boltStore{
		cachedFK: map[string][]byte{
			"stderr": []byte("stderr.file"),
			"stdout": []byte("stdout.file"),
		},
		cachedOK: map[string][]byte{
			"stderr": []byte("stderr.offset"),
			"stdout": []byte("stdout.offset"),
		},
	}
}

func (s *boltStore) Initialize() error {
	db, err := bolt.Open("local/my.db", 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return err
	}
	s.db = db
	return nil
}

func (s *boltStore) Close() error {
	return s.db.Close()
}

func (s *boltStore) AllocationStorer(allocID string) storage.Storer {
	return &boltAllocStorer{
		store:   s,
		allocID: allocID,
	}
}

type boltAllocStorer struct {
	store   *boltStore
	allocID string
}

func (s *boltAllocStorer) Set(task, stream string, info *storage.Info) {
	s.store.db.Update(func(tx *bolt.Tx) error {
		b, _ := tx.CreateBucketIfNotExists([]byte(fmt.Sprintf("%s.%s", s.allocID, task)))
		if err := b.Put(s.store.cachedFK[stream], []byte(info.File)); err != nil {
			return err
		}
		byteValue := make([]byte, 8)
		binary.BigEndian.PutUint64(byteValue, uint64(info.Offset))
		return b.Put(s.store.cachedOK[stream], byteValue)
	})
}

func (s *boltAllocStorer) Get(task, stream string) *storage.Info {
	var info *storage.Info

	s.store.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(fmt.Sprintf("%s.%s", s.allocID, task)))
		if b == nil {
			return nil
		}
		fileBytes := b.Get(s.store.cachedFK[stream])
		if fileBytes == nil {
			return nil
		}
		info = &storage.Info{
			File: string(fileBytes),
		}
		byteValue := b.Get(s.store.cachedOK[stream])
		info.Offset = int64(binary.BigEndian.Uint64(byteValue))
		return nil
	})
	return info
}
