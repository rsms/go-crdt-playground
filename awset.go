package crdt

import (
	"fmt"
	"sort"
	"strings"
)

// AWSet implements a state-based OR-Set CRDT (aka Optimized AW-Set aka OR-SWOT.)
//
//   OR = Observed Remove
//   AW = concurrent Add Wins over delete
//   SWOT = Set WithOut Tombstone
//   CRDT = Conflict-free Replicated Data Type
//
// An OR-Set allows adding and removal of elements with minimal state overhead,
// compared to more conventional CRDTs. If an element is both added and removed
// concurrently, i.e. by different actors, the add wins.
//
// We use a version vector for the whole set. When an element is added to the set,
// the version vector is incremented and the `(Actor, count)` pair for that
// increment is stored against the element as its "birth dot". For example,
// Set{Actor:3,Count:2}.Add("Alice") causes the entry "Alice"=(3,2) to be stored.
// Every time the element is updated or (re)added to the set, its "dot" is updated
// to that of the current `(Actor, count)`. When an element is removed it is simply
// removed without any "tombstone."
//
// When an element exists in replica A and not replica B, is it because A added it
// and B has not yet seen that, or that B removed it and A has not yet seen that?
// Usually the presence of a tombstone arbitrates. In this implementation we
// compare the "birth dot" of the present element to the clock in the Set it is
// absent in. If the element dot is not "seen" by the Set clock, that means the
// other set has yet to see this add, and the item is in the merged Set. If the Set
// clock dominates the dot, that means the other Set has removed this element
// already, and the item is not in the merged Set.
//
// References:
//
//   Talk by Russell Brown of Riak at Erlang Factory in Stockholm, 2016.
//   http://www.erlang-factory.com/static/upload/media/
//   1474729847848977russellbrownbiggersetseuc2016.pdf
//
//   A comprehensive study of Convergent and Commutative Replicated Data Types
//   http://hal.upmc.fr/inria-00555588/
//
//   An Optimized Conﬂict-free Replicated Set
//   http://arxiv.org/abs/1210.3368
//
//   Dotted Version Vectors: Logical Clocks for Optimistic Replication
//   http://arxiv.org/abs/1011.5808
//
//   Riak DT ORSWOT
//   https://github.com/basho/riak_dt/blob/develop/src/riak_dt_orswot.erl
//
type AWSet struct {
	Actor
	VersionVector
	Entries map[string]Dot
}

func (s *AWSet) SortedValues() []string {
	values := make([]string, len(s.Entries))
	i := 0
	for value := range s.Entries {
		values[i] = value
		i++
	}
	sort.Strings(values)
	return values
}

func (s *AWSet) Reset() {
	s.VersionVector = VersionVector{0}
	s.Entries = make(map[string]Dot)
}

func (s *AWSet) Clone() *AWSet {
	c := &AWSet{Actor: s.Actor}
	c.VersionVector = s.VersionVector.Clone()
	c.Entries = make(map[string]Dot, len(s.Entries))
	for k, v := range s.Entries {
		c.Entries[k] = v
	}
	return c
}

func (s *AWSet) Has(k string) bool { _, ok := s.Entries[k]; return ok }

func (s *AWSet) Add(k ...string) {
	for _, k := range k {
		s.VersionVector[s.Actor]++
		s.Entries[k] = Dot{s.Actor, s.VersionVector[s.Actor]}
	}
}

func (s *AWSet) Del(k ...string) {
	// s.VersionVector[s.Actor]++
	for _, k := range k {
		delete(s.Entries, k)
	}
}

func (s *AWSet) Merge(src *AWSet) {
	s.merge(src.VersionVector, src.Entries)
}

func (s *AWSet) merge(srcVersionVector VersionVector, srcEntries map[string]Dot) {
	// merge dst <- src
	logOutcome := func(phase int, k string, dstDot, srcDot *Dot, outcome string) {
		dots := "() <- ()"
		if dstDot != nil && srcDot != nil {
			dots = fmt.Sprintf("%v <- %v", dstDot, srcDot)
		} else if srcDot != nil {
			dots = fmt.Sprintf("() <- %v", srcDot)
		} else if dstDot != nil {
			dots = fmt.Sprintf("%v <- ()", dstDot)
		}
		fmt.Printf("> phase %d %-10q %-18s => %s\n", phase, k, dots, outcome)
	}
	dst := s
	fmt.Printf("\nmerge %v <- %v\n", s.VersionVector, srcVersionVector)
	for k, srcDot := range srcEntries {
		if dstDot, ok := dst.Entries[k]; ok {
			// dst contains this entry
			if dstDot != srcDot {
				logOutcome(1, k, &dstDot, &srcDot, "update")
			} else {
				logOutcome(1, k, &dstDot, &srcDot, "keep")
			}
		} else {
			// dst may have seen this entry, but doesn't contain it at the moment.
			// Decide if we should add it or skip it.
			if dst.VersionVector.HasDot(srcDot) {
				logOutcome(1, k, nil, &srcDot, "skip")
				continue
			} else {
				// the src version counter of the entry is larger than the counter for the src actor
				// in our prior knowledge.
				logOutcome(1, k, nil, &srcDot, "add")
			}
		}
		dst.Entries[k] = srcDot
	}
	// process deleted
	for k, dstDot := range dst.Entries {
		if srcDot, ok := srcEntries[k]; ok {
			// in both dst and src; keep
			logOutcome(2, k, &dstDot, &srcDot, "keep")
		} else {
			// If src has witnessed this entry and it is no longer present in src,
			// it means we should remove it. (dstDot here is the current "local" version of the entry.)
			if srcVersionVector.HasDot(dstDot) {
				logOutcome(2, k, &dstDot, nil, "remove")
				delete(dst.Entries, k)
			} else {
				logOutcome(2, k, &dstDot, nil, "keep")
			}
		}
	}
	dst.VersionVector.Merge(srcVersionVector)
}

func (s AWSet) String() string {
	var sb strings.Builder
	sb.WriteString(s.VersionVector.String())
	for _, value := range s.SortedValues() {
		v := s.Entries[value]
		fmt.Fprintf(&sb, "\n  %s  %q", v, value)
	}
	return sb.String()
}
