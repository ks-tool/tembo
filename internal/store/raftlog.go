package store

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"

	"github.com/ks-tool/tembo/internal/raft"

	bolt "go.etcd.io/bbolt"
)

var (
	bucketRaftLog   = []byte("raftlog")
	bucketRaftState = []byte("raftstate")
	keyTerm         = []byte("term")
	keyVotedFor     = []byte("voted_for")
	keyBaseIndex    = []byte("base_index")
	keyBaseTerm     = []byte("base_term")
	// keyConfig holds the raft membership in effect at the base entry, as
	// JSON; the key is absent until a configuration entry is compacted or a
	// snapshot installed (the bootstrap set applies then).
	keyConfig = []byte("config")
)

// RaftStorage returns the raft.Storage that keeps the term, the vote and the
// log of the raft node in the same BoltDB file as the state machine, so that
// compacting the log and installing a snapshot of the state machine are each
// one transaction. Every write is committed to disk before it returns.
func (s *Store) RaftStorage() raft.Storage { return raftStorage{s.db} }

type raftStorage struct{ db *bolt.DB }

// An entry is stored under its big-endian index, as the 8 bytes of its term
// followed by its data.
func indexKey(index uint64) []byte { return binary.BigEndian.AppendUint64(nil, index) }

func (r raftStorage) Load() (st raft.Stored, err error) {
	err = r.db.View(func(tx *bolt.Tx) error {
		state := tx.Bucket(bucketRaftState)
		if v := state.Get(keyTerm); len(v) == 8 {
			st.Term = binary.BigEndian.Uint64(v)
		}
		st.VotedFor = string(state.Get(keyVotedFor))
		if v := state.Get(keyBaseIndex); len(v) == 8 {
			st.Base.Index = binary.BigEndian.Uint64(v)
		}
		if v := state.Get(keyBaseTerm); len(v) == 8 {
			st.Base.Term = binary.BigEndian.Uint64(v)
		}
		if v := state.Get(keyConfig); v != nil {
			m := new(raft.Membership)
			if err := json.Unmarshal(v, m); err != nil {
				return fmt.Errorf("raft state: config: %w", err)
			}
			st.Config = m
		}
		return tx.Bucket(bucketRaftLog).ForEach(func(k, v []byte) error {
			if len(k) != 8 || len(v) < 8 {
				return fmt.Errorf("raft log: malformed entry %x", k)
			}
			st.Log = append(st.Log, raft.Entry{
				Index: binary.BigEndian.Uint64(k),
				Term:  binary.BigEndian.Uint64(v),
				Data:  append([]byte(nil), v[8:]...), // Bolt memory is only valid inside the tx
			})
			return nil
		})
	})
	return st, err
}

func (r raftStorage) SaveState(term uint64, votedFor string) error {
	return r.db.Update(func(tx *bolt.Tx) error {
		state := tx.Bucket(bucketRaftState)
		if err := state.Put(keyTerm, binary.BigEndian.AppendUint64(nil, term)); err != nil {
			return err
		}
		return state.Put(keyVotedFor, []byte(votedFor))
	})
}

func (r raftStorage) SetLog(from uint64, entries []raft.Entry) error {
	return r.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketRaftLog)
		c := b.Cursor()
		// Seeking again after each delete avoids Next skipping a key.
		for k, _ := c.Seek(indexKey(from)); k != nil; k, _ = c.Seek(indexKey(from)) {
			if err := c.Delete(); err != nil {
				return err
			}
		}
		for i, e := range entries {
			if e.Index != from+uint64(i) {
				return fmt.Errorf("raft log: entry %d stored at %d", e.Index, from+uint64(i))
			}
			v := binary.BigEndian.AppendUint64(nil, e.Term)
			if err := b.Put(indexKey(e.Index), append(v, e.Data...)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (r raftStorage) Compact(base raft.Entry, config raft.Membership) error {
	return r.db.Update(func(tx *bolt.Tx) error {
		if applied := readIndex(tx.Bucket(bucketMeta)); applied < base.Index {
			return fmt.Errorf("raft log: compacting to %d, past the applied %d", base.Index, applied)
		}
		if err := trimLog(tx.Bucket(bucketRaftLog), base.Index); err != nil {
			return err
		}
		return putBase(tx.Bucket(bucketRaftState), base, config)
	})
}

// Snapshot is the key/value bucket as a JSON object. Both are read in one
// transaction, so they belong together.
func (r raftStorage) Snapshot() (data []byte, applied uint64, err error) {
	err = r.db.View(func(tx *bolt.Tx) error {
		kv := make(map[string][]byte)
		err := tx.Bucket(bucketKV).ForEach(func(k, v []byte) error {
			kv[string(k)] = v
			return nil
		})
		if err != nil {
			return err
		}
		applied = readIndex(tx.Bucket(bucketMeta))
		data, err = json.Marshal(kv) // before the transaction ends: v is Bolt memory
		return err
	})
	return data, applied, err
}

func (r raftStorage) InstallSnapshot(base raft.Entry, keepLog bool, config raft.Membership, data []byte) error {
	var kv map[string][]byte
	if err := json.Unmarshal(data, &kv); err != nil {
		return fmt.Errorf("snapshot: %w", err)
	}
	return r.db.Update(func(tx *bolt.Tx) error {
		if err := tx.DeleteBucket(bucketKV); err != nil {
			return err
		}
		b, err := tx.CreateBucket(bucketKV)
		if err != nil {
			return err
		}
		for k, v := range kv {
			if err = b.Put([]byte(k), v); err != nil {
				return err
			}
		}
		if err = tx.Bucket(bucketMeta).Put(keyApplied, indexKey(base.Index)); err != nil {
			return err
		}
		upTo := base.Index
		if !keepLog {
			upTo = math.MaxUint64
		}
		if err = trimLog(tx.Bucket(bucketRaftLog), upTo); err != nil {
			return err
		}
		return putBase(tx.Bucket(bucketRaftState), base, config)
	})
}

// trimLog removes the entries up to index upTo.
func trimLog(b *bolt.Bucket, upTo uint64) error {
	c := b.Cursor()
	// First again after each delete avoids Next skipping a key.
	for k, _ := c.First(); k != nil && binary.BigEndian.Uint64(k) <= upTo; k, _ = c.First() {
		if err := c.Delete(); err != nil {
			return err
		}
	}
	return nil
}

func putBase(state *bolt.Bucket, base raft.Entry, config raft.Membership) error {
	if err := state.Put(keyBaseIndex, indexKey(base.Index)); err != nil {
		return err
	}
	if err := state.Put(keyBaseTerm, indexKey(base.Term)); err != nil {
		return err
	}
	data, err := json.Marshal(config)
	if err != nil {
		return err
	}
	return state.Put(keyConfig, data)
}
