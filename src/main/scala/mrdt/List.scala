package mrdt

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.immutable.List as ScalaList

class List[A: Ordering](val mem: Set[A], val ordering: HashMap[A, Int])
    extends Mergeable[List[A]],
      scala.collection.immutable.Iterable[A] {
  def this() = this(new Set(), new HashMap())

  override def size: Int = mem.size
  override def toSeq: Seq[A] = mem.toSeq
  def contains(value: A): Boolean = mem.contains(value)

  def indexOf(value: A): Option[Int] = {
    ordering.get(value)
  }

  def add(value: A): Boolean = {
    val inserted = mem.insert(value)
    if (inserted) {
      ordering.put(value, mem.size)
    }
    inserted
  }

  def insert(idx: Int, value: A): Boolean = {
    ordering.foreach { (value, i) =>
      if (i >= idx) {
        ordering.put(value, i + 1)
      }
    }
    ordering.put(value, idx)
    mem.insert(value)
  }

  def remove(value: A): Boolean = {
    val idx = this.indexOf(value)
    if (idx.isEmpty) {
      return false
    }

    mem.remove(value)
    ordering.remove(value)
    ordering.foreach { (value, i) =>
      if (i >= idx.get) {
        ordering.put(value, i - 1)
      }
    }
    true
  }

  override def clone(): List[A] = new List[A](mem.clone(), ordering.clone())

  override def merge(replica1: List[A], replica2: List[A]): List[A] = {
    // Merge mem
    val mergeMem = mem.merge(replica1.mem, replica2.mem)

    // Transform ordering into mathematical representation
    val baseOrd = Set(Utils.mapToOrdering(this.ordering))
    val ord1 = Set(Utils.mapToOrdering(replica1.ordering))
    val ord2 = Set(Utils.mapToOrdering(replica2.ordering))

    // Merge ordering
    val mergeOrd = baseOrd.merge(ord1, ord2)

    val mergeOrdSet = mergeOrd.asSet
    // Remove all the pairs that have at least one element that
    // was removed from the original list
    mergeOrdSet.foreach { pair =>
      if (!mergeMem.contains(pair._1) || !mergeMem.contains(pair._2)) {
        mergeOrd.remove(pair)
      }
    }

    // Transform mathematical representation into ordering
    val ordering = Utils.orderingToHashMap(mergeOrdSet)

    new List[A](mergeMem, ordering)
  }

  def iterator: Iterator[A] = mem.iterator
}

object List {
  def of[A: Ordering](values: A*): List[A] = {
    val newList = new List[A]()
    for ((value, idx) <- values.zipWithIndex) {
      newList.insert(idx, value)
    }
    newList
  }
}
