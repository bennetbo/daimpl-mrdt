package mrdt

package mrdt

import org.scalatest.funsuite.AnyFunSuite

class OrdTests extends AnyFunSuite {
  test("Ord.empty") {
    assert(new Ord[Int]().size === 0)
  }

  test("Ord.insert") {
    val ord = new Ord[Int]()
    ord.insert(0, 1)
    ord.insert(1, 2)
    ord.insert(2, 3)

    assert(ord.size === 3)
    assert(ord.indexOf(1) == Some(0))
    assert(ord.indexOf(2) == Some(1))
    assert(ord.indexOf(3) == Some(2))
  }

  test("Ord.toOrderedSet") {
    val ord = new Ord[Int]()
    ord.insert(0, 1)
    ord.insert(1, 2)
    ord.insert(2, 3)

    val set = ord.toOrderedSet

    assert(set.size === 3)
    assert(set.contains((1, 2)))
    assert(set.contains((1, 3)))
    assert(set.contains((2, 3)))
  }
}
