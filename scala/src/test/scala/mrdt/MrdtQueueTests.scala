package mrdt

import org.scalatest.funsuite.AnyFunSuite

class MrdtQueueTests extends AnyFunSuite {
  test("MrdtQueue.empty") {
    assert(new MrdtQueue[Int]().size === 0)
  }

  test("MrdtQueue.push") {
    var queue = MrdtQueue
      .of(1, 2, 3)
      .push(4)

    assert(queue.size === 4)
    assert(queue.indexOf(1) == Some(0))
    assert(queue.indexOf(2) == Some(1))
    assert(queue.indexOf(3) == Some(2))
    assert(queue.indexOf(4) == Some(3))
  }

  test("MrdtQueue.insert_duplicate") {
    var queue = new MrdtQueue[Int]()
      .push(1)
      .push(1)

    assert(queue.size === 1)
    assert(queue.contains(1))
  }

  test("MrdtQueue.pop") {
    var queue = MrdtQueue
      .of(1, 2, 3)
      .pop()
      ._2

    assert(queue.size === 2)
    assert(queue.indexOf(2) == Some(0))
    assert(queue.indexOf(3) == Some(1))
  }

  test("MrdtQueue.merge") {
    val queue = MrdtQueue.of(1, 2, 3)

    val replica1 = queue.push(4)

    val replica2 = queue.pop()._2.push(5)

    val mergedList = queue.merge(replica1, replica2)

    assert(mergedList.size == 4)
    assert(mergedList.indexOf(2) == Some(0))
    assert(mergedList.indexOf(3) == Some(1))
    assert(mergedList.indexOf(4) == Some(2))
    assert(mergedList.indexOf(5) == Some(3))
  }
}
