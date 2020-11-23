package crdt

import (
  "fmt"
  "os"
  "sort"
  "testing"
)

func TestAWSetXXX(t *testing.T) {
  A, B, printstate, assertEntries := testAWSetInit(t)

  A.Add("A", "B", "C")
  B.Add("A", "B", "C")
  A.Merge(B)
  B.Merge(A)
  printstate()
  assertEntries(A, "A", "B", "C")
  assertEntries(B, "A", "B", "C")

  A.Del("B")
  B.Add("B")
  B.Merge(A)
  printstate()
  A.Merge(B)
  printstate()
  assertEntries(A, "A", "B", "C")
  assertEntries(B, "A", "B", "C") // concurrent writer wins
}

func TestAWSet(t *testing.T) {
  A, B, printstate, assertEntries := testAWSetInit(t)

  // empty
  assertEntries(A)
  assertEntries(B)

  A.Add("Shelly")
  printstate()
  assertEntries(A, "Shelly")
  assertEntries(B)

  B.Merge(A) // B <- A
  printstate()
  assertEntries(A, "Shelly")
  assertEntries(B, "Shelly")

  B.Add("Bob", "Phil", "Pete")
  assertEntries(A, "Shelly")
  assertEntries(B, "Shelly", "Bob", "Phil", "Pete")

  A.Merge(B) // A <- B
  printstate()
  assertEntries(A, "Shelly", "Bob", "Phil", "Pete")
  assertEntries(B, "Shelly", "Bob", "Phil", "Pete")

  A.Del("Phil")
  A.Add("Bob") // update
  A.Add("Anna")
  assertEntries(A, "Shelly", "Bob" /*   */, "Pete", "Anna")
  assertEntries(B, "Shelly", "Bob", "Phil", "Pete")

  B.Merge(A) // B <- A
  printstate()
  assertEntries(A, "Shelly", "Bob", "Pete", "Anna")
  assertEntries(B, "Shelly", "Bob", "Pete", "Anna")

  A.Del("Bob", "Pete")
  B.Del("Bob", "Shelly")
  A.Merge(B) // A <- B
  B.Merge(A) // B <- A
  printstate()
  assertEntries(A, "Anna")
  assertEntries(B, "Anna")

  A.Add("A", "B", "C")
  A.Del("A")
  A.Add("A")
  B.Merge(A) // B <- A
  printstate()
  assertEntries(A, "Anna", "A", "B", "C")
  assertEntries(B, "Anna", "A", "B", "C")
}

func TestAWSetConcurrentAddWinsOverDelete(t *testing.T) {
  // A property of the OR-Set is that concurrent add wins over delete.
  // Illustrated:
  //   --------------------------------------|----------------------------------------
  //   Actor A                               | Actor B
  //   {"Anne"(A 1), "Bob"(A 1)}             | {"Anne"(B 1)}
  //                                         |
  //   Del("Bob") -> {"Anne"(A 1)}           | Add("Bob") -> {"Anne"(B 1), "Bob"(B 2)}
  //                                         |
  //   Merge(B) -> {"Anne"(A 1), "Bob"(B 2)} | Merge(A) -> {"Anne"(B 1), "Bob"(B 2)}
  //   --------------------------------------|----------------------------------------
  //
  // Writer wins; "Bob" is restored. (Actor B is the writer.)
  //
  A, B, printstate, assertEntries := testAWSetInit(t)

  A.Add("Anne", "Bob")
  B.Add("Anne")
  // fork state and test concurrent add and delete:
  if A, B := A.Clone(), B.Clone(); A != nil {
    B.Add("Bob")
    A.Del("Bob")
    B.Merge(A)
    A.Merge(B)
    printstate()
    assertEntries(B, "Anne", "Bob") // writer wins
    assertEntries(A, "Anne", "Bob")
  }
  // now let's try merge before delete: (i.e. non-concurrent delete)
  B.Add("Bob")
  B.Merge(A) // comment-out this line to switch to concurrent delete (test will fail)
  A.Del("Bob")
  B.Merge(A)
  A.Merge(B)
  printstate()
  assertEntries(B, "Anne")
  assertEntries(A, "Anne")
}

func TestAWSetCommutativity(t *testing.T) {
  A, B, printstate, assertEntries := testAWSetInit(t)
  A.Add("Shelly", "Bob", "Pete", "Anna")
  B.Add("Shelly", "Bob", "Pete", "Anna")

  // Test commutativity (merge order doesn't matter.)
  // A removes "Anna" while B adds/updates "Anna".
  A.Del("Anna")
  B.Add("Anna")
  assertEntries(A, "Shelly", "Bob", "Pete")
  assertEntries(B, "Shelly", "Bob", "Pete", "Anna")
  // The outcome should be that "Anna" is restored (undeleted.)
  expectedAfterMerge := []string{"Shelly", "Bob", "Pete", "Anna"}
  //
  // We try different merge orders to ensure the results are the same.
  // Merge order: A -> B -> A
  if A, B := A.Clone(), B.Clone(); A != nil {
    B.Merge(A) // B <- A
    A.Merge(B) // A <- B
    assertEntries(A, expectedAfterMerge...)
    assertEntries(B, expectedAfterMerge...)
  }
  // Merge order: B -> A -> B
  A.Merge(B) // A <- B
  B.Merge(A) // B <- A
  assertEntries(A, expectedAfterMerge...)
  assertEntries(B, expectedAfterMerge...)
  printstate()

  os.Exit(0)
}

func testAWSetInit(
  t *testing.T,
) (A, B *AWSet, printstate func(), assertEntries func(*AWSet, ...string) bool) {
  A = &AWSet{
    Actor:         0,
    VersionVector: VersionVector{0, 0},
    Entries:       make(map[string]Dot),
  }
  B = &AWSet{
    Actor:         1,
    VersionVector: VersionVector{0, 0},
    Entries:       make(map[string]Dot),
  }
  printstate = func() {
    fmt.Printf("————————————————————————————————————————————————\n")
    fmt.Printf("Replica A: %s\n", A)
    fmt.Printf("Replica B: %s\n", B)
    fmt.Printf("————————————————————————————————————————————————\n")
  }
  assertEntries = func(s *AWSet, expectedValues ...string) bool {
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
