package mrdt

import org.scalatest.funsuite.AnyFunSuite

class QueueTests extends AnyFunSuite {
  test("Queue.empty") {
    assert(new Queue[Int]().size === 0)
  }

  test("Queue.push") {
    var queue = Queue.of(1, 2, 3)
    queue.push(4)

    assert(queue.size === 4)
    assert(queue.indexOf(1) == Some(0))
    assert(queue.indexOf(2) == Some(1))
    assert(queue.indexOf(3) == Some(2))
    assert(queue.indexOf(4) == Some(3))
  }

  test("Queue.insert_duplicate") {
    var queue = new Queue[Int]()
    queue.push(1)
    queue.push(1)

    assert(queue.size === 1)
    assert(queue.contains(1))
  }

  test("Queue.pop") {
    var queue = Queue.of(1, 2, 3)
    queue.pop()

    assert(queue.size === 2)
    assert(queue.indexOf(2) == Some(0))
    assert(queue.indexOf(3) == Some(1))
  }

  test("Queue.merge") {
    val queue = Queue.of(1, 2, 3)

    val replica1 = queue.clone()
    replica1.push(4)

    val replica2 = queue.clone()
    replica2.pop()
    replica2.push(5)

    val mergedList = queue.merge(replica1, replica2)

    assert(mergedList.size == 4)
    assert(mergedList.indexOf(2) == Some(0))
    assert(mergedList.indexOf(3) == Some(1))
    assert(mergedList.indexOf(4) == Some(2))
    assert(mergedList.indexOf(5) == Some(3))
  }
}
