package mrdt

import scala.collection.immutable.HashSet

trait Mergeable[A] {
  def merge(replica1: A, replica2: A): A
}

trait MemMergeable[A, B] {
  def merge(replica1: A, replica2: A, mergedMem: MrdtSet[B]): A
}

class MrdtSet[A](val store: Set[A]) extends Iterable[A], Mergeable[MrdtSet[A]] {
  def this() = this(new HashSet[A]())

  override def size: Int = store.size
  override def toSeq: Seq[A] = store.toSeq
  def contains(value: A): Boolean = store.contains(value)
  def insert(value: A): MrdtSet[A] = new MrdtSet[A](store + value)
  def remove(value: A): MrdtSet[A] = new MrdtSet[A](store - value)

  override def clone(): MrdtSet[A] = new MrdtSet[A](store)

  override def merge(replica1: MrdtSet[A], replica2: MrdtSet[A]): MrdtSet[A] = {
    val mergedStore = store.filter(value =>
      replica1.contains(value) && replica2.contains(value)
    ) ++
      replica1.store.filter(!store.contains(_)) ++
      replica2.store.filter(!store.contains(_))
    new MrdtSet[A](mergedStore)
  }

  def asSet: Set[A] = this.store

  def iterator: Iterator[A] = store.iterator
}

object MrdtSet {
  def of[A](values: A*): MrdtSet[A] = new MrdtSet[A](HashSet(values: _*))
}
