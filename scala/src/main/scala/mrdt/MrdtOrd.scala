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
    val mergeOrd =
      this.toOrderedSet.merge(replica1.toOrderedSet, replica2.toOrderedSet)

    val mergeOrdSet = mergeOrd.asSet
    val filteredOrdSet = mergeOrdSet.filter { pair =>
      mergedMem.contains(pair._1) && mergedMem.contains(pair._2)
    }

    val ordering = Utils.orderingToHashMap(filteredOrdSet)

    new MrdtOrd[A](ordering)
  }

  def iterator: Iterator[(A, Int)] = store.iterator
}
