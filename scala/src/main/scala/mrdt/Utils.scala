package mrdt

import scala.collection.immutable.{HashMap, HashSet, List, Set}
import scala.collection.immutable.{SortedSet, TreeSet}
import scala.math.Ordering

object Utils {
  def mapToOrdering[A: Ordering](
      ordering: HashMap[A, Int]
  ): Set[(A, A)] = {
    ordering.foldLeft(Set.empty[(A, A)]) { case (acc, (value1, idx1)) =>
      ordering.foldLeft(acc) { case (innerAcc, (value2, idx2)) =>
        if (idx1 < idx2) innerAcc + ((value1, value2))
        else innerAcc
      }
    }
  }

  def orderingToHashMap[A: Ordering](
      ordering: Set[(A, A)]
  ): HashMap[A, Int] = {
    implicit val ord: Ordering[List[A]] = Ordering.Implicits.seqOrdering

    // Prepare auxiliary structures
    val nodes: Set[A] =
      ordering
        .flatMap(pair => List(pair._1, pair._2))
        .toSet
    val adjacencyList =
      scala.collection.mutable.Map.empty[A, List[A]].withDefaultValue(Nil)
    val inDegree =
      scala.collection.mutable.Map.empty[A, Int].withDefaultValue(0)

    // Prepare adjacency list and in-degree count
    ordering.foreach { case (from, to) =>
      adjacencyList(from) =
        (adjacencyList(from) :+ to).sorted // Keep lists sorted
      inDegree(to) = inDegree(to) + 1
    }

    // Priority Queue for maintaining lexical order among available nodes
    val queue = scala.collection.mutable.PriorityQueue
      .empty[A](implicitly[Ordering[A]].reverse)

    // Enqueue nodes with no in-degrees
    nodes.filter(inDegree(_) == 0).foreach(queue.enqueue(_))

    var result = List.empty[A]

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

    result.zipWithIndex.foldLeft(HashMap.empty[A, Int]) {
      case (map, (value, idx)) =>
        map + (value -> idx)
    }
  }
}
