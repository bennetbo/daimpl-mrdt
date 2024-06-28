package mrdt

import scala.collection.mutable.HashMap

class Ord[A: Ordering](val store: HashMap[A, Int])
    extends Iterable[(A, Int)],
      MemMergeable[Ord[A], A] {
  def this() = this(new HashMap())

  override def size: Int = store.size
  def contains(value: A): Boolean = store.contains(value)
  def toOrderedSet: Set[(A, A)] = Set(Utils.mapToOrdering(store))

  def indexOf(value: A): Option[Int] = {
    store.get(value)
  }

  def insert(idx: Int, value: A) = {
    store.foreach { (value, i) =>
      if (i >= idx) {
        store.put(value, i + 1)
      }
    }
    store.put(value, idx)
  }

  // TODO we could improve lookup time here by storing a hash map from index to value?
  def removeAt(idx: Int): Option[A] = {
    store
      .find((value, i) => i == idx)
      .map((value, idx) => {
        store.remove(value)
        store.foreach { (value, i) =>
          if (i >= idx) {
            store.put(value, i - 1)
          }
        }
        value
      })
  }

  override def clone(): Ord[A] = new Ord[A](store.clone())

  override def merge(
      replica1: Ord[A],
      replica2: Ord[A],
      mergedMem: Set[A]
  ): Ord[A] = {
    // Transform ordering into mathematical representation and merge
    val mergeOrd =
      this.toOrderedSet.merge(replica1.toOrderedSet, replica2.toOrderedSet)

    val mergeOrdSet = mergeOrd.asSet
    // Remove all the pairs that have at least one element that
    // was removed from the original list
    mergeOrdSet.foreach { pair =>
      if (!mergedMem.contains(pair._1) || !mergedMem.contains(pair._2)) {
        mergeOrd.remove(pair)
      }
    }

    // Transform mathematical representation into ordering
    val ordering = Utils.orderingToHashMap(mergeOrdSet)

    new Ord[A](ordering)
  }

  def iterator: Iterator[(A, Int)] = store.iterator
}
