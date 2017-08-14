package robadger

import (
	"os"
	"io/ioutil"
	"github.com/dgraph-io/badger"
	"github.com/rohanthewiz/serr"
	"github.com/rohanthewiz/roencoding"
)

type Store struct {
	store *badger.KV
	// If we used a temp dir, store it here so we can remove it on close
	tempDir string
}

// Create a new key-value store for badger.
// If no directories are supplied, a temp directory will be created i.e. data is volatile!
// If only one directory is supplied, it is used as the key and value stores
// If more than 1 directory is supplied, the first will be used as the key store, and the second as the value store
// It is the client's responsibility to Close() the store. Note that close also returns an err value
// which should be checked, because some data is flushed on close. Example:
//		defer func() {
//			err = store.Close()
//			if err != nil {
//				Log("Error", "Error closing store - some values may not have been saved", "error", err.Error())
//		}()
func NewStore(dirs ...string) (st *Store, err error) {
	bopt := badger.DefaultOptions
	tmp_dir := ""
	len_dirs := len(dirs)
	if len_dirs == 0 {
		tmp_dir, err = ioutil.TempDir("/tmp", "badger")
		if err != nil {
			return nil, serr.Wrap(err, "Error creating temporary directory for badger store")
		}

		bopt.Dir = tmp_dir
		bopt.ValueDir = tmp_dir
	} else if len_dirs >= 1 {
		bopt.Dir = dirs[0]
		if len_dirs == 1 {
			bopt.ValueDir = dirs[0]
		} else {
			bopt.ValueDir = dirs[1]
		}
	}
	st = new(Store)
	st.tempDir = tmp_dir
	s, err := badger.NewKV(&bopt)
	if err != nil {
		return nil, serr.Wrap(err, "Unable to create a badger key-value store")
	}
	st.store = s
	return
}

func (s *Store) Close() error {
	err := s.store.Close()
	if err != nil {
		return err
	}
	if s.tempDir != "" {
		err = os.RemoveAll(s.tempDir)  // be a good citizen
	}
	return err
}

func (s *Store) SetString(key, val string) error {
	return s.store.Set([]byte(key), []byte(val))
}

func (s *Store) GetString(key string) (out string, err error) {
	var item badger.KVItem
	err =  s.store.Get([]byte(key), &item)
	if err != nil {
		return
	}
	out = string(item.Value())
	return
}

func (s *Store) SetBytes(k, v []byte) error {
	return s.store.Set(k, v)
}

func (s *Store) GetBytes(k []byte) (out []byte, err error) {
	var item badger.KVItem
	err = s.store.Get(k, &item)
	if err != nil {
		return
	}
	out = item.Value()
	return
}

// Add a hashed key to the store if it doesn't already exist
func (s *Store) TouchHashed(in string) (err error) {
	return s.store.Touch([]byte(roencoding.XXHash(in)))
}

// Does hash of key exist in the store?
func (s *Store) ExistsHashed(in string) (exists bool, err error) {
	return s.store.Exists([]byte(roencoding.XXHash(in)))
}
