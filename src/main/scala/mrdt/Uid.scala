package mrdt

import java.util.Base64

val UID_SIZE = 64

case class Uid(value: Array[Byte])

object Uid {
  private val random = new java.util.Random()

  val zero = 0

  def gen: Uid =
    val randomBytes = new Array[Byte](UID_SIZE)
    random.nextBytes(randomBytes)
    Uid(randomBytes)
}
