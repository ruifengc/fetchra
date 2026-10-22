package store

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"path/filepath"
	"time"

	"github.com/RyanChen-cn/fetchra/internal/model"
	bolt "go.etcd.io/bbolt"
)

var tasks = []byte("tasks")
var events = []byte("events")
var settings = []byte("settings")
var requests = []byte("requests")

type Request struct {
	Digest string `json:"digest"`
	TaskID string `json:"task_id"`
}
type Store struct{ db *bolt.DB }

func Open(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}
	db, err := bolt.Open(filepath.Join(dir, "state.db"), 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, name := range [][]byte{tasks, events, settings, requests} {
			if _, e := tx.CreateBucketIfNotExists(name); e != nil {
				return e
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db}, nil
}
func (s *Store) Close() error { return s.db.Close() }
func (s *Store) Load() ([]model.Task, model.Policy, error) {
	list := []model.Task{}
	p := model.DefaultPolicy()
	err := s.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket(settings).Get([]byte("policy")); b != nil {
			if e := json.Unmarshal(b, &p); e != nil {
				return e
			}
		}
		return tx.Bucket(tasks).ForEach(func(k, v []byte) error {
			var t model.Task
			if e := json.Unmarshal(v, &t); e != nil {
				return e
			}
			list = append(list, t)
			return nil
		})
	})
	return list, p, err
}
func (s *Store) Save(t model.Task, kind string, key string, request *Request) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b, e := json.Marshal(t)
		if e != nil {
			return e
		}
		if kind == "removed" {
			e = tx.Bucket(tasks).Delete([]byte(t.ID))
		} else {
			e = tx.Bucket(tasks).Put([]byte(t.ID), b)
		}
		if e != nil {
			return e
		}
		if request != nil {
			b, _ := json.Marshal(request)
			if e = tx.Bucket(requests).Put([]byte(key), b); e != nil {
				return e
			}
		}
		if kind == "" {
			return nil
		}
		bucket := tx.Bucket(events)
		seq, e := bucket.NextSequence()
		if e != nil {
			return e
		}
		ev := model.Event{Cursor: seq, TaskID: t.ID, Kind: kind, State: t.State, At: time.Now().UTC()}
		b, _ = json.Marshal(ev)
		var id [8]byte
		binary.BigEndian.PutUint64(id[:], seq)
		if e = bucket.Put(id[:], b); e != nil {
			return e
		}
		if seq > 10000 {
			binary.BigEndian.PutUint64(id[:], seq-10000)
			return bucket.Delete(id[:])
		}
		return nil
	})
}
func (s *Store) Request(key string) (*Request, error) {
	var r *Request
	err := s.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket(requests).Get([]byte(key)); b != nil {
			r = &Request{}
			return json.Unmarshal(b, r)
		}
		return nil
	})
	return r, err
}
func (s *Store) Policy(p model.Policy) error {
	return s.db.Update(func(tx *bolt.Tx) error { b, _ := json.Marshal(p); return tx.Bucket(settings).Put([]byte("policy"), b) })
}
func (s *Store) Events(after uint64, taskID string, limit int) ([]model.Event, uint64, error) {
	list := []model.Event{}
	next := after
	err := s.db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket(events)
		c := bucket.Cursor()
		first, _ := c.First()
		if after > 0 && first != nil && after < binary.BigEndian.Uint64(first)-1 {
			return model.Err("CURSOR_EXPIRED", "query a fresh task snapshot before reading events")
		}
		if after > bucket.Sequence() {
			return model.Err("INVALID_ARGUMENT", "event cursor is ahead of the service")
		}
		var id [8]byte
		binary.BigEndian.PutUint64(id[:], after+1)
		for k, v := c.Seek(id[:]); k != nil; k, v = c.Next() {
			var ev model.Event
			if e := json.Unmarshal(v, &ev); e != nil {
				return e
			}
			next = ev.Cursor
			if taskID == "" || taskID == ev.TaskID {
				list = append(list, ev)
			}
			if len(list) >= limit {
				break
			}
		}
		return nil
	})
	return list, next, err
}
