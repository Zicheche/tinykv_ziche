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
	"errors"
	"math/rand"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

type GrantStatus uint64

const (
	Grant GrantStatus = iota
	Reject
	Pending
)

type VoteStatus uint64

const (
	VoteWin VoteStatus = iota
	VoteLose
	VotePending
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64
	Vote uint64

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]GrantStatus

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout           int
	randomizedElectionTimeout int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64

	step func(r *Raft, m pb.Message) bool
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	// Your Code Here (2A).
	hs, _, _ := c.Storage.InitialState()
	initialTerm := hs.Term
	initialVote := hs.Vote
	r := &Raft{
		id:               c.ID,
		Prs:              make(map[uint64]*Progress),
		votes:            make(map[uint64]GrantStatus),
		heartbeatTimeout: c.HeartbeatTick,
		electionTimeout:  c.ElectionTick,
		RaftLog:          newLog(c.Storage),
		Vote:             initialVote,
		Term:             initialTerm,
	}
	r.resetRandomizedElectionTimeout()
	for _, id := range c.peers {
		r.Prs[id] = &Progress{
			Match: 0,
			Next:  r.RaftLog.LastIndex() + 1,
		}
	}
	r.becomeFollower(initialTerm, None)

	return r
}

func (r *Raft) resetRandomizedElectionTimeout() {
	r.electionElapsed = 0
	r.randomizedElectionTimeout = rand.Int()%r.electionTimeout + r.electionTimeout
}

func stepFollower(r *Raft, m pb.Message) bool {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.requestVotes()
	case pb.MessageType_MsgAppend:
		r.handleAppendEntries(m)
	case pb.MessageType_MsgHeartbeat:
		r.handleHeartbeat(m)
	}
	return false
}

func stepCandidate(r *Raft, m pb.Message) bool {
	switch m.MsgType {
	case pb.MessageType_MsgHup:
		r.becomeCandidate()
		r.requestVotes()
	case pb.MessageType_MsgRequestVoteResponse:
		r.handleVoteResponse(m)
	}
	return false
}

func stepLeader(r *Raft, m pb.Message) bool {
	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
	case pb.MessageType_MsgPropose:
		r.handleClientMsg(m)
		r.requestAppends(m)
	case pb.MessageType_MsgAppendResponse:
		r.handleAppendResponse(m)
	case pb.MessageType_MsgHeartbeatResponse:
		r.handleHeartBeatResponse(m)
	}
	return false
}

func (r *Raft) handleHeartBeatResponse(m pb.Message) {
	lastIndex := r.RaftLog.LastIndex()
	lastTerm, _ := r.RaftLog.Term(lastIndex)
	if !m.Reject && r.Prs[m.From].Match < lastIndex {
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			From:    r.id,
			To:      m.From,
			Term:    m.Term,
			Index:   lastIndex,
			LogTerm: lastTerm,
		})
	}
}
func (r *Raft) handleAppendResponse(m pb.Message) {
	if m.Term == r.Term {
		if !m.Reject {
			// update follower pr's match and next
			r.Prs[m.From].Match = m.Index
			r.Prs[m.From].Next = m.Index + 1

			// commit when current term equals current term
			LogTerm, _ := r.RaftLog.Term(m.Index)
			if LogTerm == r.Term {
				r.UpdateCommit()
			}
		} else {
			preterm, _ := r.RaftLog.Term(m.Index - 1)
			//println("return entires: ", len(m.Entries))
			entries := []*pb.Entry{&r.RaftLog.entries[m.Index-(r.RaftLog.entries[0].Index)]}
			entries = append(entries, m.Entries...)
			//println(len(entries))
			//for i, ent := range entries{
			//	println("entry " ,i ," is", ent.Term, ent.Index)
			//}
			r.msgs = append(r.msgs, pb.Message{
				MsgType: pb.MessageType_MsgAppend,
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Index:   m.Index - 1,
				LogTerm: preterm,
				Entries: entries,
				Commit:  r.RaftLog.committed,
			})
		}
	}

}

func (r *Raft) UpdateCommit() {
	length := len(r.Prs)
	if length > 1 {
		matches := make([]int, 0, 0)
		for _, pr := range r.Prs {
			matches = append(matches, int(pr.Match))
		}
		sort.Ints(matches)

		NewCommitted := uint64(matches[(length-1)/2])
		if NewCommitted > r.RaftLog.committed {
			r.RaftLog.committed = NewCommitted
			r.bcastCommitted()
		}
	}
}

func (r *Raft) bcastCommitted() {
	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		lastIndex := r.RaftLog.LastIndex()
		lastTerm, _ := r.RaftLog.Term(lastIndex)
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			From:    r.id,
			To:      id,
			Term:    r.Term,
			Index:   r.RaftLog.LastIndex(),
			LogTerm: lastTerm,
			Commit:  r.RaftLog.committed,
		})
	}
}
func (r *Raft) handleVoteResponse(m pb.Message) {
	if r.Term == m.Term {
		if !m.Reject {
			r.votes[m.From] = Grant
		} else {
			r.votes[m.From] = Reject
		}
	}

	res := r.winElection()
	if res == VoteWin {
		r.becomeLeader()
	} else if res == VoteLose {
		r.becomeFollower(r.Term, None)
	}
}

func (r *Raft) winElection() VoteStatus {
	numPeers := len(r.Prs)
	quorum := numPeers/2 + 1
	grants := 0
	rejects := 0
	for _, grant := range r.votes {
		if grant == Grant {
			grants += 1
		} else if grant == Reject {
			rejects += 1
		}
		if grants >= quorum {
			return VoteWin
		}
		if rejects >= quorum {
			return VoteLose
		}
	}
	return VotePending
}

func (r *Raft) requestAppends(m pb.Message) {
	index := r.RaftLog.LastIndex() - uint64(len(m.Entries))
	for _, ent := range m.Entries {
		index++
		ent.Index = index
		ent.Term = r.Term
	}

	preIndex := r.RaftLog.LastIndex() - uint64(len(m.Entries))

	var preTerm uint64
	if preIndex == 0 {
		preTerm = r.Term
	} else {
		preTerm, _ = r.RaftLog.Term(preIndex)
	}

	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgAppend,
			From:    r.id,
			To:      id,
			Term:    r.Term,
			Index:   preIndex,
			LogTerm: preTerm,
			Entries: m.Entries,
			Commit:  r.RaftLog.committed,
		})
	}
}
func (r *Raft) requestVotes() {
	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		var entries []*pb.Entry
		for _, ent := range r.RaftLog.entries {
			entries = append(entries, &ent)
		}
		lastIndex := r.RaftLog.LastIndex()
		logterm, _ := r.RaftLog.Term(lastIndex)
		r.msgs = append(r.msgs, pb.Message{
			MsgType: pb.MessageType_MsgRequestVote,
			From:    r.id,
			To:      id,
			Term:    r.Term,
			Index:   lastIndex,
			LogTerm: logterm,
			Entries: entries,
		})
	}
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) {
	// Your Code Here (2A).
	preIndex := r.RaftLog.LastIndex() - 1
	var preTerm uint64
	if preIndex == 0 {
		preTerm = r.Term
	} else {
		preTerm, _ = r.RaftLog.Term(preIndex)
	}

	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppend,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Index:   preIndex,
		LogTerm: preTerm,
		Entries: []*pb.Entry{{Term: r.Term,
			Index: preIndex + 1,
		}},
		Commit: r.RaftLog.committed,
	})
}

func (r *Raft) commitNoop() {
	// Your Code Here (2A).
	r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Index: r.RaftLog.LastIndex() + 1, Term: r.Term})

	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
	index := r.RaftLog.LastIndex()
	r.Prs[r.id].Match = index
	r.Prs[r.id].Next = index + 1
	for id := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

func (r *Raft) bcastHeartbeat() {
	//println("leader send heartbeat")
	for id, _ := range r.Prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id)
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		To:      to,
		Term:    r.Term,
		Commit:  min(r.Prs[to].Match, r.RaftLog.committed),
	})
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	if r.State == StateLeader {
		r.heartbeatElapsed += 1
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.Step(pb.Message{
				MsgType: pb.MessageType_MsgBeat,
			})
			r.heartbeatElapsed = 0
		}
	}

	if r.State != StateLeader {
		r.electionElapsed += 1
		if r.electionElapsed > r.randomizedElectionTimeout {
			r.Step(
				pb.Message{
					MsgType: pb.MessageType_MsgHup,
				})
			r.electionElapsed = 0
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	if term > r.Term {
		r.Vote = None // question: why this become none?
	}
	r.Term = term
	r.Lead = lead
	r.State = StateFollower
	r.step = stepFollower

	r.resetRandomizedElectionTimeout()
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.Term += 1
	r.State = StateCandidate
	r.step = stepCandidate
	r.resetRandomizedElectionTimeout()
	r.resetVotes()
	r.Vote = r.id
	r.votes[r.id] = Grant

	if len(r.Prs) == 1 {
		r.becomeLeader()
	}
}

func (r *Raft) resetVotes() {
	for id, _ := range r.Prs {
		r.votes[id] = Pending
	}
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	if r.State != StateLeader {
		r.State = StateLeader
		r.step = stepLeader
		for id, _ := range r.Prs {
			r.Prs[id].Match = 0
			r.Prs[id].Next = r.RaftLog.LastIndex() + 1
		}
		r.commitNoop()
	}
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).

	// set the step function for r.state
	switch r.State {
	case StateLeader:
		r.step = stepLeader
	case StateCandidate:
		r.step = stepCandidate
	case StateFollower:
		r.step = stepFollower
	}

	if m.Term > r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	if m.Term < r.Term && m.Term != 0 {
		if m.MsgType == pb.MessageType_MsgRequestVote || m.MsgType == pb.MessageType_MsgAppend ||
			m.MsgType == pb.MessageType_MsgHeartbeat {
			resp := pb.Message{
				MsgType: convertToResponse(m.MsgType),
				From:    r.id,
				To:      m.From,
				Term:    r.Term,
				Reject:  true,
			}
			r.msgs = append(r.msgs, resp)
			return nil
		}
	}

	if m.MsgType == pb.MessageType_MsgRequestVote {
		r.Lead = None
		resp := pb.Message{
			MsgType: pb.MessageType_MsgRequestVoteResponse,
			From:    r.id,
			To:      m.From,
			Term:    r.Term,
			Reject:  true,
		}
		index := r.RaftLog.LastIndex()
		logTerm, _ := r.RaftLog.Term(index)
		if (r.Vote == None &&
			(m.LogTerm > logTerm || (m.LogTerm == logTerm && m.Index >= index))) ||
			r.Vote == m.From {
			resp.Reject = false
			r.Vote = m.From
		}
		r.msgs = append(r.msgs, resp)
	}

	if m.MsgType == pb.MessageType_MsgAppend && m.Term == r.Term {
		r.becomeFollower(m.Term, m.From)
	}
	r.step(r, m)
	return nil
}

func (r *Raft) handleClientMsg(m pb.Message) {
	// Your Code Here (2A).
	index := r.RaftLog.LastIndex()
	for _, entry := range m.Entries {
		index++
		r.RaftLog.entries = append(r.RaftLog.entries, pb.Entry{Index: index, Term: r.Term, Data: entry.Data})
	}

	if len(r.Prs) == 1 {
		r.RaftLog.committed = r.RaftLog.LastIndex()
	}
	r.Prs[r.id].Match = r.RaftLog.LastIndex()
	r.Prs[r.id].Next = r.RaftLog.LastIndex() + 1
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Index == 0 {
		r.appendEntries(m)
		r.RaftLog.committed = m.Commit
	} else { // index != 0 , compare the preIndex and preTerm
		match := r.appendAccept(m)
		if !match {
			resp := pb.Message{
				MsgType: pb.MessageType_MsgAppendResponse,
				From:    r.id,
				To:      m.From,
				Term:    m.Term,
				Index:   m.Index,
				Entries: m.Entries,
				Reject:  true,
			}
			r.msgs = append(r.msgs, resp)
		} else {
			r.appendEntries(m)
			if m.Commit > r.RaftLog.committed {
				r.RaftLog.committed = min(m.Commit, m.Index+uint64(len(m.Entries)))
			}
		}
	}
}
func (r *Raft) appendAccept(m pb.Message) bool {
	term, _ := r.RaftLog.Term(m.Index)
	return term == m.LogTerm
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).

	if m.Commit > r.RaftLog.committed {
		r.RaftLog.committed = m.Commit
	}
	r.msgs = append(r.msgs, pb.Message{
		MsgType: convertToResponse(m.MsgType),
		From:    r.id,
		To:      m.From,
		Term:    r.Term,
		Commit:  r.RaftLog.committed,
	})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) appendEntries(m pb.Message) {
	lastIndex := r.RaftLog.LastIndex()
	var firstIndex uint64 = 0
	if lastIndex != 0 {
		firstIndex = r.RaftLog.entries[0].Index
	}

	conflict := false // check conflict between leader and follower

	for _, entry := range m.Entries { // append entries
		if entry.Index <= r.RaftLog.LastIndex() { // overlap entries
			term, _ := r.RaftLog.Term(entry.Index)
			if entry.Term != term {
				conflict = true
				// if conflicted, change the stabled
				r.RaftLog.stabled = entry.Index - 1
				r.RaftLog.entries[entry.Index-firstIndex] = *entry
			}
		} else {
			r.RaftLog.entries = append(r.RaftLog.entries, *entry)
		}
	}

	// if conflicted, truncate the entries beyond the leader
	if conflict {
		lastIndex := m.Index + uint64(len(m.Entries))
		r.RaftLog.entries = r.RaftLog.entries[:lastIndex-firstIndex+1]
	}

	// send response
	r.msgs = append(r.msgs, pb.Message{
		MsgType: pb.MessageType_MsgAppendResponse,
		From:    r.id,
		To:      m.From,
		Term:    m.Term,
		Index:   m.Index + uint64(len(m.Entries)),
	})

}
