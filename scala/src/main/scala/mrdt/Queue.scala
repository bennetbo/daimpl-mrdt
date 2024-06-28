package mrdt

class Queue[A: Ordering](val store: List[A])
    extends Iterable[A],
      Mergeable[Queue[A]] {
  def this() = this(new List[A]())

  def push(value: A) = store.add(value)
  def pop(): Option[A] = store.removeAt(0)

  def contains(value: A): Boolean = store.contains(value)
  def indexOf(value: A): Option[Int] = store.indexOf(value)

  override def size: Int = store.size
  override def isEmpty: Boolean = store.isEmpty
  override def toSeq: Seq[A] = store.toSeq
  override def clone(): Queue[A] = new Queue[A](store.clone())

  def iterator: Iterator[A] = store.iterator

  override def merge(replica1: Queue[A], replica2: Queue[A]): Queue[A] = {
    val result = store.merge(replica1.store, replica2.store)
    new Queue(result)
  }
}

object Queue {
  def of[A: Ordering](values: A*): Queue[A] = new Queue[A](List.of(values: _*))
}
