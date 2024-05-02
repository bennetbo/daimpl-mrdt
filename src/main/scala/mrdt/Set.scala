package mrdt

import scala.collection.mutable.HashSet

trait Mergeable[A] {
  def merge(replica1: A, replica2: A): A
}

class Set[A](val store: HashSet[A]) extends Iterable[A], Mergeable[Set[A]] {
  def this() = this(new HashSet[A]())

  override def size: Int = store.size
  override def toSeq: Seq[A] = store.toSeq
  def contains(value: A): Boolean = store.contains(value)
  def insert(value: A): Boolean = store.add(value)
  def remove(value: A): Boolean = store.remove(value)

  override def clone(): Set[A] = new Set[A](store.clone())

  override def merge(replica1: Set[A], replica2: Set[A]): Set[A] = {
    val newSet = new Set[A]()
    for (value <- store) {
      if (replica1.contains(value) && replica2.contains(value)) {
        newSet.insert(value)
      }
    }
    for (value <- replica1.store) {
      if (!store.contains(value)) {
        newSet.insert(value)
      }
    }
    for (value <- replica2.store) {
      if (!store.contains(value)) {
        newSet.insert(value)
      }
    }
    newSet
  }

  def asSet: HashSet[A] = this.store

  def iterator: Iterator[A] = store.iterator
}

object Set {
  def of[A](values: A*): Set[A] = new Set[A](HashSet(values: _*))
}
