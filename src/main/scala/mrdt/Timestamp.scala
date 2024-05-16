package mrdt

type Timestamp = Long

object Timestamp:
  def zero(): Timestamp = 0L
  def inc(t: Timestamp): Timestamp = t + 1
