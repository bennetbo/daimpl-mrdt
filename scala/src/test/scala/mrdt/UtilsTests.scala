package mrdt

import org.scalatest.funsuite.AnyFunSuite
import scala.collection.immutable.Set
import scala.collection.immutable.HashMap

class UtilsTests extends AnyFunSuite {
  test("Utils.mapToOrdering") {
    val result =
      Utils.mapToOrdering(HashMap(1 -> 2, 2 -> 1, 3 -> 0))

    assert(result.size == 3)
    assert(result.contains((3, 2)))
    assert(result.contains((3, 1)))
    assert(result.contains((2, 1)))
  }

  test("Utils.orderingToHashMap") {
    val result =
      Utils.orderingToHashMap(Set((2, 3), (5, 3), (5, 2), (4, 3), (4, 2)))

    assert(result.size == 4)
    assert(result(4) == 0)
    assert(result(5) == 1)
    assert(result(2) == 2)
    assert(result(3) == 3)
  }
}
