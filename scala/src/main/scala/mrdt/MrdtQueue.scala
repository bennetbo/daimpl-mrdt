package mrdt

class MrdtQueue[A: Ordering](val store: MrdtList[A])
    extends Iterable[A],
      Mergeable[MrdtQueue[A]] {
  def this() = this(new MrdtList[A]())

  def push(value: A): MrdtQueue[A] = new MrdtQueue(store.add(value))
  def pop(): (Option[A], MrdtQueue[A]) = {
    val (value, newStore) = store.removeAt(0)
    (value, new MrdtQueue(newStore))
  }

  def contains(value: A): Boolean = store.contains(value)
  def indexOf(value: A): Option[Int] = store.indexOf(value)

  override def size: Int = store.size
  override def isEmpty: Boolean = store.isEmpty
  override def toSeq: Seq[A] = store.toSeq
  override def clone(): MrdtQueue[A] = new MrdtQueue[A](store.clone())

  def iterator: Iterator[A] = store.iterator

  override def merge(
      replica1: MrdtQueue[A],
      replica2: MrdtQueue[A]
  ): MrdtQueue[A] = {
    val result = store.merge(replica1.store, replica2.store)
    new MrdtQueue(result)
  }
}

object MrdtQueue {
  def of[A: Ordering](values: A*): MrdtQueue[A] =
    new MrdtQueue[A](MrdtList.of(values: _*))
}
