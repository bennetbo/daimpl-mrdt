package mrdt

import scala.annotation.tailrec

case class VectorClock(timestamps: Map[Uid, Timestamp]) {
  def timeOf(uid: Uid): Timestamp = timestamps.getOrElse(uid, Timestamp.zero())

  def inc(uid: Uid): VectorClock =
    VectorClock(timestamps.updated(uid, Timestamp.inc(timeOf(uid))))

  def size: Int = timestamps.size
  def isEmpty: Boolean = timestamps.isEmpty

  def <=(o: VectorClock): Boolean = timestamps.forall: (k, v) =>
    o.timestamps.get(k) match
      case None        => false
      case Some(other) => v <= other
  def <(o: VectorClock): Boolean = this <= o && timestamps.exists: (k, v) =>
    o.timestamps.get(k) match
      case None        => false
      case Some(other) => v < other
}

object VectorClock {
  def zero(): VectorClock = VectorClock(Map.empty)
  def fromMap(map: Map[Uid, Timestamp]): VectorClock = VectorClock(map)

  def merge(left: VectorClock, right: VectorClock): VectorClock = {
    val timestamps = Map(
      left.timestamps
        .zip(right.timestamps)
        .map((l, r) => (l._1, l._2 max r._2))
        .toSeq: _*
    )
    VectorClock.fromMap(timestamps)
  }

  def lca(left: VectorClock, right: VectorClock): VectorClock = {
    val timestamps = Map(
      left.timestamps
        .zip(right.timestamps)
        .map((l, r) => (l._1, l._2 min r._2))
        .toSeq: _*
    )
    VectorClock.fromMap(timestamps)
  }

  val totalOrdering: Ordering[VectorClock] =
    new Ordering[VectorClock] {
      override def compare(x: VectorClock, y: VectorClock): Int =
        ordering.tryCompare(x, y) match
          case Some(v) => v
          case None =>
            @tailrec
            def smaller(remaining: scala.collection.immutable.List[Uid]): Int =
              remaining match {
                case h :: t =>
                  val l = x.timestamps.get(h)
                  val r = y.timestamps.get(h)
                  val res = Ordering[Option[Timestamp]].compare(l, r)
                  if res == 0 then smaller(t) else res
                case Nil =>
                  0
              }
            val ids =
              (x.timestamps.keysIterator ++ y.timestamps.keysIterator).toList.sorted
            smaller(ids)
    }

  given ordering: PartialOrdering[VectorClock] =
    new PartialOrdering[VectorClock] {
      override def tryCompare(x: VectorClock, y: VectorClock): Option[Int] = {
        if x < y then return Some(-1)
        if y < x then return Some(1)
        if x <= y && y <= x then return Some(0)
        None
      }

      override def lteq(x: VectorClock, y: VectorClock): Boolean = x <= y
    }
}
