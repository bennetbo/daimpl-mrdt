package mrdt

import scala.collection.immutable.{HashMap, HashSet, List}

class MrdtList[A: Ordering](val mem: MrdtSet[A], val ord: MrdtOrd[A])
    extends Iterable[A],
      Mergeable[MrdtList[A]] {
  def this() = this(new MrdtSet(), new MrdtOrd())

  override def size: Int = mem.size
  override def toSeq: Seq[A] = mem.toSeq
  def contains(value: A): Boolean = mem.contains(value)
  def indexOf(value: A): Option[Int] = ord.indexOf(value)

  def add(value: A): MrdtList[A] = {
    val newMem = mem.insert(value)
    val newOrd = if (newMem != mem) ord.insert(mem.size, value) else ord
    new MrdtList[A](newMem, newOrd)
  }

  def insert(idx: Int, value: A): MrdtList[A] = {
    val newOrd = ord.insert(idx, value)
    val newMem = mem.insert(value)
    new MrdtList[A](newMem, newOrd)
  }

  def removeAt(idx: Int): (Option[A], MrdtList[A]) = {
    val (value, newOrd) = ord.removeAt(idx)
    value match {
      case Some(value) =>
        val newMem = mem.remove(value)
        (Some(value), new MrdtList[A](newMem, newOrd))
      case None => (None, this)
    }
  }

  def remove(value: A): MrdtList[A] = {
    indexOf(value)
      .map(idx => removeAt(idx)._2)
      .getOrElse(this)
  }

  override def clone(): MrdtList[A] = this

  override def merge(
      replica1: MrdtList[A],
      replica2: MrdtList[A]
  ): MrdtList[A] = {
    val mergeMem = mem.merge(replica1.mem, replica2.mem)
    val mergeOrd = ord.merge(replica1.ord, replica2.ord, mergeMem)

    new MrdtList[A](mergeMem, mergeOrd)
  }

  def iterator: Iterator[A] = mem.iterator
}

object MrdtList {
  def of[A: Ordering](values: A*): MrdtList[A] = {
    values.foldLeft(new MrdtList[A]())((list, value) => list.add(value))
  }
}
