package mrdt

import scala.collection.immutable.{HashMap, HashSet, List, Set}
import scala.collection.immutable.{SortedSet, TreeSet}
import scala.math.Ordering

object Utils {
  def mapToOrdering[A: Ordering](
      ordering: HashMap[A, Int]
  ): Set[(A, A)] = {
    val sortedItems = ordering.toList.sortBy(_._2)
    sortedItems.sliding(2).foldLeft(Set.empty[(A, A)]) {
      case (acc, List((prevValue, _), (currValue, _))) =>
        acc + ((prevValue, currValue))
    }
  }

  def toposort[T: Ordering](
      pairs: Set[(T, T)]
  ): List[T] = {
    val graph =
      scala.collection.mutable.Map[T, List[T]]().withDefaultValue(List.empty)
    val inDegree = scala.collection.mutable.Map[T, Int]().withDefaultValue(0)
    val allNodes = pairs.flatMap { case (from, to) => Set(from, to) }

    // Build the graph and calculate in-degrees
    pairs.foreach { case (from, to) =>
      graph(from) = graph(from) :+ to
      inDegree(to) += 1
    }

    // Use a PriorityQueue as a min-heap based on (in-degree, node value)
    val queue = scala.collection.mutable.PriorityQueue[(Int, T)]().reverse
    allNodes.foreach(node => queue.enqueue((inDegree(node), node)))

    val result = scala.collection.mutable.ListBuffer[T]()
    val processed = scala.collection.mutable.Set[T]()

    while (queue.nonEmpty) {
      val (_, node) = queue.dequeue()
      if (!processed.contains(node)) {
        result += node
        processed += node

        graph(node).foreach { neighbor =>
          if (!processed.contains(neighbor)) {
            inDegree(neighbor) -= 1
            queue.enqueue((inDegree(neighbor), neighbor))
          }
        }
      }
    }

    result.toList
  }
}
