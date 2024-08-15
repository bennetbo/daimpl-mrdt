package mrdt

import org.scalatest.funsuite.AnyFunSuite
import scala.collection.immutable.Set
import scala.collection.immutable.HashMap

class UtilsTests extends AnyFunSuite {
  test("Utils.mapToOrdering") {
    val result =
      Utils.mapToOrdering(HashMap(1 -> 0, 2 -> 1, 3 -> 2))

    assert(result.size == 2)
    assert(result.contains((1, 2)))
    assert(result.contains((2, 3)))
  }

  test("Utils.toposort") {
    // lca: 2, 3
    // left: 4, 2, 3
    // right: 5, 2, 3
    // result: 4, 5, 2, 3

    val result =
      Utils.toposort(Set((2, 3), (5, 2), (4, 2)))

    assert(result == List(4, 5, 2, 3))
  }
}
