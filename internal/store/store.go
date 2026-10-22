package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/ks-tool/tembo/internal/raft"

	"github.com/rs/zerolog/log"
	bolt "go.etcd.io/bbolt"
)

// Command is what travels through the raft log. Keep it self-describing: the
// state machine must be able to apply it years later without extra context.
type Command struct {
	Op    string `json:"op"` // "set", "add", "del" or "merge"
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

var (
	bucketKV   = []byte("kv")
	bucketMeta = []byte("meta")
	keyApplied = []byte("applied")

	// ErrNotFound is returned by Get for a missing key.
	ErrNotFound = errors.New("key not found")
)

// Store is the replicated state machine: a key/value bucket in BoltDB plus the
// index of the last applied raft entry, written in the same transaction. The
// raft log lives in the same file, see RaftStorage.
type Store struct {
	db *bolt.DB
}

// Open opens (or creates) the BoltDB file at path.
func Open(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketKV, bucketMeta, bucketRaftLog, bucketRaftState} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// Close releases the database file.
func (s *Store) Close() error { return s.db.Close() }

// Apply is the raft.Config.Apply callback.
//
// The entry's index is stored in the same Bolt transaction as its effect, so
// an entry that reaches the store a second time — after a restart the raft
// node hands over the entries since the applied index it was given, and a
// leader replays the log to a node that lost its own — is applied exactly
// once. Committed entries never change, so skipping by index is safe.
func (s *Store) Apply(e raft.Entry) {
	err := s.db.Update(func(tx *bolt.Tx) error {
		meta := tx.Bucket(bucketMeta)
		if e.Index <= readIndex(meta) {
			return nil // already applied
		}
		// An entry without data is a no-op of raft, only counted as applied.
		if len(e.Data) > 0 {
			if err := applyCommand(tx.Bucket(bucketKV), e.Data); err != nil {
				// A malformed command must not wedge the log: record it as
				// handled and move on.
				log.Warn().Err(err).Uint64("index", e.Index).Msg("store: entry skipped")
			}
		}
		return meta.Put(keyApplied, binary.BigEndian.AppendUint64(nil, e.Index))
	})
	if err != nil {
		// The state machine no longer matches the log; nothing sensible is
		// left to do in-process.
		log.Fatal().Err(err).Uint64("index", e.Index).Msg("store: apply entry")
	}
}

// ApplyCommand applies cmd directly, without a raft entry: the standalone
// mode has no log to go through.
func (s *Store) ApplyCommand(cmd Command) error {
	data, err := json.Marshal(cmd)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error {
		return applyCommand(tx.Bucket(bucketKV), data)
	})
}

func applyCommand(kv *bolt.Bucket, data []byte) error {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return fmt.Errorf("decode: %w", err)
	}
	switch cmd.Op {
	case "set":
		return kv.Put([]byte(cmd.Key), []byte(cmd.Value))
	case "add": // set only if the key is absent
		if kv.Get([]byte(cmd.Key)) != nil {
			return nil
		}
		return kv.Put([]byte(cmd.Key), []byte(cmd.Value))
	case "del":
		return kv.Delete([]byte(cmd.Key))
	case "merge": // Value is a JSON object merged into the one at Key; null removes a field
		return mergeJSON(kv, []byte(cmd.Key), []byte(cmd.Value))
	}
	return fmt.Errorf("unknown op %q", cmd.Op)
}

func mergeJSON(kv *bolt.Bucket, key, patch []byte) error {
	var p map[string]json.RawMessage
	if err := json.Unmarshal(patch, &p); err != nil {
		return fmt.Errorf("merge: %w", err)
	}
	obj := make(map[string]json.RawMessage)
	if v := kv.Get(key); v != nil {
		if err := json.Unmarshal(v, &obj); err != nil {
			return fmt.Errorf("merge into %q: %w", key, err)
		}
	}
	for k, v := range p {
		if string(v) == "null" {
			delete(obj, k)
		} else {
			obj[k] = v
		}
	}
	b, err := json.Marshal(obj) // sorted keys: every node stores the same bytes
	if err != nil {
		return err
	}
	return kv.Put(key, b)
}

// Get returns the local value of key. On a follower it may lag the leader.
func (s *Store) Get(key string) (string, error) {
	var val []byte
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketKV).Get([]byte(key))
		if v == nil {
			return ErrNotFound
		}
		val = append([]byte(nil), v...) // Bolt memory is only valid inside the tx
		return nil
	})
	return string(val), err
}

// AppliedIndex returns the index of the last raft entry applied to the store.
func (s *Store) AppliedIndex() (uint64, error) {
	var idx uint64
	err := s.db.View(func(tx *bolt.Tx) error {
		idx = readIndex(tx.Bucket(bucketMeta))
		return nil
	})
	return idx, err
}

func readIndex(meta *bolt.Bucket) uint64 {
	if v := meta.Get(keyApplied); len(v) == 8 {
		return binary.BigEndian.Uint64(v)
	}
	return 0
}
