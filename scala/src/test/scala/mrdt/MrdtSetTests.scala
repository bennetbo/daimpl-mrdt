package mrdt

import org.scalatest.funsuite.AnyFunSuite

class MrdtSetTests extends AnyFunSuite {
  test("MrdtSet.empty") {
    assert(new MrdtSet[Int]().size === 0)
  }

  test("MrdtSet.insert") {
    var set = new MrdtSet[Int]()
      .insert(1)
      .insert(1)

    assert(set.size === 1)
    assert(set.contains(1))
  }

  test("MrdtSet.remove") {
    var set = MrdtSet
      .of(1, 2, 3)
      .remove(1)

    assert(set.size === 2)
    assert(!set.contains(1))
    assert(set.contains(2))
    assert(set.contains(3))
  }

  test("MrdtSet.merge") {
    val set = new MrdtSet[Int]()
      .insert(1)
      .insert(2)
      .insert(3)

    val replica1 = set.insert(4)

    val replica2 = set.remove(1).insert(5)

    val mergedSet = set.merge(replica1, replica2)

    assert(mergedSet.toSeq == MrdtSet.of(2, 3, 4, 5).toSeq)
  }
}
