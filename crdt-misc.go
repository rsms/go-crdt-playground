package crdt

import (
  "fmt"
  "strings"
)

// Actor is a 0-based identifier for a specific actor
type Actor uint // aka "Client"

// Dot represents a specific version of an actor
type Dot struct {
  Actor
  Counter uint
}

func (v Dot) String() string {
  return fmt.Sprintf("(%c %d)", 'A'+v.Actor, v.Counter)
}

// -----------

type VersionVector []uint // Actor => version

// HasDot returns true if d is contained within the set vv.
// VersionVector{1,3,2}.HasDot(Dot{1,2}) => true, since 3>=2
// VersionVector{1,3,2}.HasDot(Dot{1,4}) => false, since 3<4
func (vv VersionVector) HasDot(d Dot) bool {
  if Actor(len(vv)) < d.Actor {
    // we have never seen d.Actor before
    return false
  }
  return vv[d.Actor] >= d.Counter
}

func (vv VersionVector) Counter(a Actor) uint {
  if Actor(len(vv)) < a {
    return 0
  }
  return vv[a]
}

func (v *VersionVector) Merge(src VersionVector) {
  dst := *v
  for i, n := range src {
    if i < len(dst) {
      if dst[i] < n {
        dst[i] = n
      }
    } else {
      dst = append(dst, n)
    }
  }
  *v = dst
}

func (v VersionVector) String() string {
  var sb strings.Builder
  sb.WriteByte('[')
  for i, n := range v {
    if i > 0 {
      sb.WriteString(", ")
    }
    fmt.Fprintf(&sb, "(%c %d)", 'A'+i, n)
  }
  sb.WriteByte(']')
  return sb.String()
}

func (v VersionVector) Clone() VersionVector {
  c := make(VersionVector, len(v))
  copy(c, v)
  return c
}
