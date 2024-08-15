package mrdt

import scala.collection.immutable.HashMap

class MrdtOrd[A: Ordering](val store: HashMap[A, Int])
    extends Iterable[(A, Int)],
      MemMergeable[MrdtOrd[A], A] {
  def this() = this(HashMap.empty)

  override def size: Int = store.size
  def contains(value: A): Boolean = store.contains(value)
  def toOrderedSet: MrdtSet[(A, A)] = MrdtSet(Utils.mapToOrdering(store))

  def indexOf(value: A): Option[Int] = {
    store.get(value)
  }

  def insert(idx: Int, value: A): MrdtOrd[A] = {
    val updatedStore = store.map { case (k, v) =>
      if (v >= idx) (k, v + 1) else (k, v)
    }
    new MrdtOrd[A](updatedStore + (value -> idx))
  }

  def removeAt(idx: Int): (Option[A], MrdtOrd[A]) = {
    store.find(_._2 == idx) match {
      case Some((value, _)) =>
        val updatedStore = store.removed(value).map { case (k, v) =>
          if (v > idx) (k, v - 1) else (k, v)
        }
        (Some(value), new MrdtOrd[A](updatedStore))
      case None => (None, this)
    }
  }

  override def clone(): MrdtOrd[A] = this

  override def merge(
      replica1: MrdtOrd[A],
      replica2: MrdtOrd[A],
      mergedMem: MrdtSet[A]
  ): MrdtOrd[A] = {
    val lcaOrdering = Utils.mapToOrdering(this.store)
    val leftOrdering = Utils.mapToOrdering(replica1.store)
    val rightOrdering = Utils.mapToOrdering(replica2.store)

    val union = leftOrdering ++ rightOrdering ++ lcaOrdering

    val set = Utils.toposort(union)

    val mergedSet = set.zipWithIndex.foldLeft(HashMap.empty[A, Int]) { case (acc, (value, idx)) =>
      if (mergedMem.contains(value)) acc + (value -> acc.size)
      else acc
    }

    MrdtOrd(mergedSet)
  }

  def iterator: Iterator[(A, Int)] = store.iterator
}
