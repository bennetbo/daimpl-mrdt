package mrdt

import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.collection.immutable.List as ScalaList

class List[A: Ordering](val mem: Set[A], val ord: Ord[A])
    extends Iterable[A],
      Mergeable[List[A]] {
  def this() = this(new Set(), new Ord())

  override def size: Int = mem.size
  override def toSeq: Seq[A] = mem.toSeq
  def contains(value: A): Boolean = mem.contains(value)

  def indexOf(value: A): Option[Int] = ord.indexOf(value)

  def add(value: A): Boolean = {
    val inserted = mem.insert(value)
    if (inserted) {
      ord.insert(mem.size - 1, value)
    }
    inserted
  }

  def insert(idx: Int, value: A): Boolean = {
    ord.insert(idx, value)
    mem.insert(value)
  }

  def removeAt(idx: Int): Option[A] = {
    ord.removeAt(idx).map { value =>
      mem.remove(value)
      value
    }
  }

  def remove(value: A): Boolean = {
    val idx = this.indexOf(value)
    if (idx.isEmpty) {
      return false
    }

    removeAt(idx.get)
    true
  }

  override def clone(): List[A] = new List[A](mem.clone(), ord.clone())

  override def merge(replica1: List[A], replica2: List[A]): List[A] = {
    val mergeMem = mem.merge(replica1.mem, replica2.mem)
    val mergeOrd = ord.merge(replica1.ord, replica2.ord, mergeMem)

    new List[A](mergeMem, mergeOrd)
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
