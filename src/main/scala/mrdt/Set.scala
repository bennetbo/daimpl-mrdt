package mrdt

import scala.collection.mutable.HashSet

trait Mergeable[A] {
  def merge(replica1: A, replica2: A): A
}

class Set[A](val storage: HashSet[A])
    extends Mergeable[Set[A]],
      scala.collection.immutable.Iterable[A] {
  def this() = this(new HashSet[A]())

  override def size: Int = storage.size
  override def toSeq: Seq[A] = storage.toSeq
  def contains(value: A): Boolean = storage.contains(value)
  def insert(value: A): Boolean = storage.add(value)
  def remove(value: A): Boolean = storage.remove(value)

  override def clone(): Set[A] = new Set[A](storage.clone())

  override def merge(replica1: Set[A], replica2: Set[A]): Set[A] = {
    val newSet = new Set[A]()
    for (value <- storage) {
      if (replica1.contains(value) && replica2.contains(value)) {
        newSet.insert(value)
      }
    }
    for (value <- replica1.storage) {
      if (!storage.contains(value)) {
        newSet.insert(value)
      }
    }
    for (value <- replica2.storage) {
      if (!storage.contains(value)) {
        newSet.insert(value)
      }
    }
    newSet
  }

  def asSet: HashSet[A] = this.storage

  def iterator: Iterator[A] = storage.iterator
}

object Set {
  def of[A](values: A*): Set[A] = new Set[A](HashSet(values: _*))
}
