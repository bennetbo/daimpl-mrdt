package mrdt

import org.scalatest.funsuite.AnyFunSuite

class SetTests extends AnyFunSuite {
  test("Set.empty") {
    assert(new Set[Int]().size === 0)
  }

  test("Set.insert") {
    var set = new Set[Int]()
    set.insert(1)
    set.insert(1)

    assert(set.size === 1)
    assert(set.contains(1))
  }

  test("Set.remove") {
    var set = Set.of(1, 2, 3)
    set.remove(1)

    assert(set.size === 2)
    assert(!set.contains(1))
    assert(set.contains(2))
    assert(set.contains(3))
  }

  test("Set.merge") {
    val set = new Set[Int]()
    set.insert(1)
    set.insert(2)
    set.insert(3)

    val replica1 = set.clone()
    replica1.insert(4)

    val replica2 = set.clone()
    replica2.remove(1)
    replica2.insert(5)

    val mergedSet = set.merge(replica1, replica2)

    assert(mergedSet.toSeq == Set.of(2, 3, 4, 5).toSeq)
  }
}
