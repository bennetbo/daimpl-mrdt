package mrdt

import scala.collection.mutable.HashSet
import scala.collection.mutable.HashMap
import scala.collection.immutable.List as ScalaList
import scala.collection.immutable.Set as ScalaSet
import scala.collection.mutable.PriorityQueue
import scala.collection.mutable
import scala.math.Ordering

object Utils {
  def mapToOrdering[A: Ordering](
      ordering: HashMap[A, Int]
  ): HashSet[(A, A)] = {
    val orderedSet = HashSet[(A, A)]()

    ordering.foreach { (value, idx) =>
      ordering.foreach { (value2, idx2) =>
        if (idx < idx2) {
          orderedSet.add((value, value2))
        }
      }
    }
    orderedSet
  }

  def orderingToHashMap[A: Ordering](
      ordering: scala.collection.Set[(A, A)]
  ): HashMap[A, Int] = {
    implicit val ord: Ordering[ScalaList[A]] = Ordering.Implicits.seqOrdering

    // Prepare auxiliary structures
    val nodes: ScalaSet[A] =
      ordering.flatMap(pair => ScalaList(pair._1, pair._2)).toSet
    val adjacencyList = mutable.Map.empty[A, ScalaList[A]].withDefaultValue(Nil)
    val inDegree = mutable.Map.empty[A, Int].withDefaultValue(0)

    // Prepare adjacency list and in-degree count
    ordering.foreach { case (from, to) =>
      adjacencyList(from) =
        (adjacencyList(from) :+ to).sorted // Keep lists sorted
      inDegree(to) = inDegree(to) + 1
    }

    // Priority Queue for maintaining lexical order among available nodes
    val queue = PriorityQueue.empty[A](implicitly[Ordering[A]].reverse)

    // Enqueue nodes with no in-degrees
    nodes.filter(inDegree(_) == 0).foreach(queue.enqueue(_))

    var result = ScalaList.empty[A]

    while (queue.nonEmpty) {
      val current = queue.dequeue()
      result = result :+ current

      adjacencyList(current).foreach { neighbor =>
        inDegree(neighbor) -= 1
        if (inDegree(neighbor) == 0) {
          queue.enqueue(neighbor)
        }
      }
    }

    val map = new HashMap[A, Int]()
    result.zipWithIndex.foreach { (value, idx) =>
      map.put(value, idx)
    }
    map
  }
}
