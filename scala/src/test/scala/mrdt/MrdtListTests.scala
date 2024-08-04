package mrdt

import org.scalatest.funsuite.AnyFunSuite

class MrdtListTests extends AnyFunSuite {
  test("MrdtList.empty") {
    assert(new MrdtList[Int]().size === 0)
  }

  test("MrdtList.insert") {
    var list = MrdtList.of(1, 2, 3).insert(0, 4)

    assert(list.size === 4)
    assert(list.indexOf(4) == Some(0))
    assert(list.indexOf(1) == Some(1))
    assert(list.indexOf(2) == Some(2))
    assert(list.indexOf(3) == Some(3))
  }

  test("MrdtList.insert_duplicate") {
    var list = new MrdtList[Int]()
      .insert(0, 1)
      .insert(0, 1)

    assert(list.size === 1)
    assert(list.contains(1))
  }

  test("MrdtList.remove") {
    var list = MrdtList.of(1, 2, 3).remove(1)

    assert(list.size === 2)
    assert(list.indexOf(2) == Some(0))
    assert(list.indexOf(3) == Some(1))
  }

  test("MrdtList.merge_add") {
    val list = MrdtList.of(1, 2, 3)

    val replica1 = list.add(4)

    val replica2 = list.remove(1).add(5)

    val mergedList = list.merge(replica1, replica2)

    assert(mergedList.size == 4)
    assert(mergedList.indexOf(2) == Some(0))
    assert(mergedList.indexOf(3) == Some(1))
    assert(mergedList.indexOf(4) == Some(2))
    assert(mergedList.indexOf(5) == Some(3))
  }

  test("MrdtList.merge_insert") {
    val list = MrdtList.of(1, 2, 3)

    val replica1 = list.insert(0, 4)

    val replica2 = list.remove(1).insert(0, 5)

    val mergedList = list.merge(replica1, replica2)

    assert(mergedList.size == 4)
    assert(mergedList.indexOf(4) == Some(0))
    assert(mergedList.indexOf(5) == Some(1))
    assert(mergedList.indexOf(2) == Some(2))
    assert(mergedList.indexOf(3) == Some(3))
  }
}
