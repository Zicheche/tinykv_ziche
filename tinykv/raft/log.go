// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	lastIndex, err := storage.LastIndex()
	if err != nil {
		return nil
	}
	if lastIndex == 0{
		//println("no record")
		return &RaftLog{storage: storage}
	} else {
		firstIndex, err := storage.FirstIndex()
		//println(firstIndex, lastIndex)
		if err != nil {
			return nil
		}

		entries, err := storage.Entries(firstIndex, lastIndex+1)
		//for _, ent := range entries{
		//	println(ent.Term, ent.Index)
		//}
		//println("pre length is :", len(entries))
		if err != nil {
			return nil
		}
		hs, _, _ := storage.InitialState()
		return &RaftLog{storage: storage, entries: entries,
			committed: hs.Commit, stabled: lastIndex}
	}
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).

	if l.stabled != 0 {
		firstIndex := l.entries[0].Index
		return l.entries[(l.stabled-firstIndex+1): ]
	} else {
		return l.entries
	}
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	if len(l.entries) != 0 {
		logIndex := l.entries[0].Index
		//println( "next entries: ", l.applied, logIndex, l.committed)
		if l.applied == 0 {
			return l.entries[: (l.committed+1-logIndex)]
		} else {
			return l.entries[(l.applied-logIndex+1) : (l.committed+1-logIndex)]
		}
	}
	return nil
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	//println(len(l.entries))
	length := len(l.entries)
	if length != 0 {
		return l.entries[length-1].Index
 	} else {
	 	return 0
	}
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	if len(l.entries) != 0 {
		logIndex := l.entries[0].Index
		if i < logIndex || logIndex + uint64(len(l.entries)) <= i {
			return 0, nil
		}
		term := l.entries[i-logIndex].Term
		return term, nil
	}
	return 0, nil
}

//func (l *RaftLog) CommittedEntries() []pb.Entry{
//	if len(l.entries) != 0 {
//		firstIndex := l.entries[0].Index
//		return l.entries[l.applied+1 - firstIndex : l.committed - firstIndex]
//	} else {
//		return nil
//	}
//}