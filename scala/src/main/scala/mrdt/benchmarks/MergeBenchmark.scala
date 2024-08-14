package benchmarks

import org.openjdk.jmh.annotations._
import java.util.concurrent.TimeUnit
import mrdt.MrdtSet
import mrdt.MrdtList
import scala.util.Random

@BenchmarkMode(Array(Mode.Throughput))
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 1, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 1, time = 1000, timeUnit = TimeUnit.MILLISECONDS)
@Fork(1)
@Threads(1)
@State(Scope.Thread)
class MergeBenchmark {
  @Param(Array("10", "100", "1000"))
  var lcaSize: Int = scala.compiletime.uninitialized

  @Param(Array("1", "10", "100"))
  var additionalOperations: Int = scala.compiletime.uninitialized

  var lca: List[Int] = scala.compiletime.uninitialized
  var left: List[Int] = scala.compiletime.uninitialized
  var right: List[Int] = scala.compiletime.uninitialized

  var lcaSet: MrdtSet[Int] = scala.compiletime.uninitialized
  var leftSet: MrdtSet[Int] = scala.compiletime.uninitialized
  var rightSet: MrdtSet[Int] = scala.compiletime.uninitialized

  var lcaList: MrdtList[Int] = scala.compiletime.uninitialized
  var leftList: MrdtList[Int] = scala.compiletime.uninitialized
  var rightList: MrdtList[Int] = scala.compiletime.uninitialized

  @Setup
  def setup(): Unit = {
    lca = Range(0, lcaSize).toList
    left = lca ++ Range(lcaSize, lcaSize + additionalOperations).toList
    right = lca ++ Range(lcaSize, lcaSize + additionalOperations).toList

    lcaSet = MrdtSet.of(lca: _*)
    leftSet = MrdtSet.of(left: _*)
    rightSet = MrdtSet.of(right: _*)

    lcaList = MrdtList.of(lca: _*)
    leftList = MrdtList.of(left: _*)
    rightList = MrdtList.of(right: _*)
  }

  @Benchmark
  def merge_set(): Unit = {
    lcaSet.merge(leftSet, rightSet)
  }

  @Benchmark
  def merge_list(): Unit = {
    lcaList.merge(leftList, rightList)
  }
}
