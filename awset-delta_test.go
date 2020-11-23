package crdt

import (
  "fmt"
  "sort"
  "testing"
)

type AWSetDelta struct {
  AWSet
  Deleted map[string]Dot
}

func (s *AWSetDelta) Del(k ...string) {
  s.VersionVector[s.Actor]++
  dot2 := Dot{s.Actor, s.VersionVector[s.Actor]}
  for _, k := range k {
    if /*dot*/ _, ok := s.Entries[k]; ok {
      // WIP decide what to delete
      // if dot.Actor != s.Actor {
      //   // removing entry from other actor
      //   fmt.Printf("* drop %q, past dot %v (record)\n", k, dot)
      if s.Deleted == nil {
        s.Deleted = make(map[string]Dot)
      }
      s.Deleted[k] = dot2
      // } else {
      //   fmt.Printf("* drop %q, past dot %v (don't record)\n", k, dot)
      // }
      delete(s.Entries, k)
    }
  }
}

func (s *AWSetDelta) Clone() *AWSetDelta {
  c := &AWSetDelta{AWSet: AWSet{Actor: s.Actor}}
  c.VersionVector = s.VersionVector.Clone()
  c.Entries = make(map[string]Dot, len(s.Entries))
  for k, v := range s.Entries {
    c.Entries[k] = v
  }
  if len(s.Deleted) > 0 {
    c.Deleted = make(map[string]Dot, len(s.Deleted))
    for k, v := range s.Deleted {
      c.Deleted[k] = v
    }
  }
  return c
}

func (s *AWSetDelta) Merge(src *AWSetDelta) {
  dst := s
  if dst.VersionVector.Counter(src.Actor) <= 0 {
    // full merge
    dst.merge(src.VersionVector, src.Entries)
    return
  }
  // dst has seen src before; just merge what has changed
  addEntries, deleted := src.MakeDeltaMergeData(dst.VersionVector)
  if addEntries != nil || deleted != nil {
    dst.deltaMerge(src.VersionVector, addEntries, deleted)
    // after the merge succeeded, src can tell dst to clean up deleted
    dst.gcDeleted(src.VersionVector)
  }
}

func (s *AWSetDelta) gcDeleted(srcVersionVector VersionVector) {
  // remove entries in s.Deleted for srcVersionVector

  // is there a way to know by just looking at the dots in s.Deleted..?

  // One way we could do this is by changing s.Deleted to include a refcounter for every actor.
  // Whenever we add something we increment the counter.

  // Another way would be to maintain a Deleted map for every known actor.
  // Cleanup then simply means removing the Deleted map for the corresponding actor.
}

func (s *AWSetDelta) MakeDeltaMergeData(
  dstVersionVector VersionVector,
) (map[string]Dot, map[string]Dot) {
  var changed map[string]Dot
  var deleted map[string]Dot
  for k, dot := range s.Entries {
    if !dstVersionVector.HasDot(dot) {
      // dst has not seen this entry
      if changed == nil {
        changed = make(map[string]Dot)
      }
      changed[k] = dot
    }
  }
  for k, dot := range s.Deleted {
    if mdot, ok := s.Entries[k]; ok && (mdot.Actor != dot.Actor || mdot.Counter > dot.Counter) {
      // removed and then added again; skip
      continue
    }
    if deleted == nil {
      deleted = make(map[string]Dot)
    }
    deleted[k] = dot
  }
  fmt.Printf("delta: changed %v, deleted %v\n", changed, deleted)
  return changed, deleted
}

func (s *AWSet) deltaMerge(
  srcVersionVector VersionVector,
  srcChanges map[string]Dot,
  srcDeleted map[string]Dot,
) {
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
  fmt.Printf("\ndeltaMerge %v <- %v\n", s.VersionVector, srcVersionVector)
  for k, srcDot := range srcChanges {
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
  for k, srcDot := range srcDeleted {
    if dstDot, ok := dst.Entries[k]; ok {
      // in both dst and src
      fmt.Printf("** %q in both; %v <- %v\n", k, dstDot, srcDot)
      if dst.VersionVector.HasDot(srcDot) {
        // entry was updated in dst; keep it
        logOutcome(2, k, nil, &srcDot, "keep")
      } else {
        logOutcome(2, k, &dstDot, nil, "remove")
        delete(dst.Entries, k)
      }
    } else {
      logOutcome(2, k, &dstDot, nil, "remove")
      delete(dst.Entries, k)
    }
  }
  dst.VersionVector.Merge(srcVersionVector)
}

func TestAWSetDelta(t *testing.T) {
  A, B, printstate, assertEntries := testAWSetDeltaInit(t)

  A.Add("A", "B")
  B.Add("A", "C")
  A.Merge(B)
  B.Merge(A)
  printstate()
  assertEntries(A, "A", "B", "C")
  assertEntries(B, "A", "B", "C")

  A.Del("B")
  A.Add("D", "E")
  B.Add("E")
  B.Merge(A)
  printstate()
  assertEntries(B, "A", "C", "D", "E")

  A.Merge(B)
  printstate()
  assertEntries(A, "A", "C", "D", "E")
}

func testAWSetDeltaInit(
  t *testing.T,
) (A, B *AWSetDelta, printstate func(), assertEntries func(*AWSetDelta, ...string) bool) {
  A = &AWSetDelta{AWSet: AWSet{
    Actor:         0,
    VersionVector: VersionVector{0, 0},
    Entries:       make(map[string]Dot),
  }}
  B = &AWSetDelta{AWSet: AWSet{
    Actor:         1,
    VersionVector: VersionVector{0, 0},
    Entries:       make(map[string]Dot),
  }}
  printstate = func() {
    fmt.Printf("————————————————————————————————————————————————\n")
    fmt.Printf("Replica A: %s\n", A)
    fmt.Printf("Replica B: %s\n", B)
    fmt.Printf("————————————————————————————————————————————————\n")
  }
  assertEntries = func(s *AWSetDelta, expectedValues ...string) bool {
    t.Helper()
    sort.Strings(expectedValues)
    actualValues := s.SortedValues()
    if len(actualValues) != len(expectedValues) {
      t.Errorf("expected %d values, got %d\nexpected: %v\ngot:      %v",
        len(expectedValues), len(actualValues),
        expectedValues, actualValues)
      t.FailNow()
      return false
    }
    for i, value := range actualValues {
      if value != expectedValues[i] {
        t.Errorf("expected values[%d] to be %q, got %q\nexpected: %v\ngot:      %v",
          i, expectedValues[i], value,
          expectedValues, actualValues)
        t.FailNow()
        return false
      }
    }
    return true
  }
  return
}
