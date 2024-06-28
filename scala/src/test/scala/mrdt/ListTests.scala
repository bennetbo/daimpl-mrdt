package mrdt

import org.scalatest.funsuite.AnyFunSuite

class ListTests extends AnyFunSuite {
  test("List.empty") {
    assert(new List[Int]().size === 0)
  }

  test("List.insert") {
    var list = List.of(1, 2, 3)
    list.insert(0, 4)

    assert(list.size === 4)
    assert(list.indexOf(4) == Some(0))
    assert(list.indexOf(1) == Some(1))
    assert(list.indexOf(2) == Some(2))
    assert(list.indexOf(3) == Some(3))
  }

  test("List.insert_duplicate") {
    var list = new List[Int]()
    list.insert(0, 1)
    list.insert(0, 1)

    assert(list.size === 1)
    assert(list.contains(1))
  }

  test("List.remove") {
    var list = List.of(1, 2, 3)
    list.remove(1)

    assert(list.size === 2)
    assert(list.indexOf(2) == Some(0))
    assert(list.indexOf(3) == Some(1))
  }

  test("List.merge_add") {
    val list = List.of(1, 2, 3)

    val replica1 = list.clone()
    replica1.add(4)

    val replica2 = list.clone()
    replica2.remove(1)
    replica2.add(5)

    val mergedList = list.merge(replica1, replica2)

    assert(mergedList.size == 4)
    assert(mergedList.indexOf(2) == Some(0))
    assert(mergedList.indexOf(3) == Some(1))
    assert(mergedList.indexOf(4) == Some(2))
    assert(mergedList.indexOf(5) == Some(3))
  }

  test("List.merge_insert") {
    val list = List.of(1, 2, 3)

    val replica1 = list.clone()
    replica1.insert(0, 4)

    val replica2 = list.clone()
    replica2.remove(1)
    replica2.insert(0, 5)

    val mergedList = list.merge(replica1, replica2)

    assert(mergedList.size == 4)
    assert(mergedList.indexOf(4) == Some(0))
    assert(mergedList.indexOf(5) == Some(1))
    assert(mergedList.indexOf(2) == Some(2))
    assert(mergedList.indexOf(3) == Some(3))
  }
}
