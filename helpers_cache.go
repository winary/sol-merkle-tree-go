package solmerkle

import (
	"encoding/binary"

	"github.com/dgraph-io/badger/v3"
)

type IndexCache struct {
	db *badger.DB
}

func NewIndexCache() (cli *IndexCache, err error) {
	db, err := badger.Open(
		badger.DefaultOptions("").WithInMemory(true),
	)
	if err != nil {
		return
	}

	cli = &IndexCache{
		db: db,
	}

	return
}

func (this *IndexCache) SetBytes2Uint64(key []byte, num uint64) (err error) {
	return this.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, uint64ToBytes(num))
	})
}

func (this *IndexCache) GetBytes2Uint64(key []byte) (num uint64, err error) {
	err = this.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if nil != err {
			return err
		}
		err = item.Value(func(val []byte) error {
			num = bytesToUint64(val)
			return nil
		})

		return nil
	})

	return
}

func (this *IndexCache) Has(key []byte) (has bool, err error) {
	err = this.db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if nil != err {
			if err == badger.ErrKeyNotFound {
				has = false
				return nil
			} else {
				return err
			}
		}
		has = true

		return nil
	})

	return
}

func uint64ToBytes(i uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], i)
	return buf[:]
}

func bytesToUint64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}
