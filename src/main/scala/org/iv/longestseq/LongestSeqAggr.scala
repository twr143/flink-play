package org.iv.longestseq

import org.apache.flink.api.common.functions.AggregateFunction
import LongestSeqAggr._
import org.slf4j.Logger

import scala.collection.mutable


/**
 * Created by twr143 on 23.01.2021 at 21:54.
 */
// another job is better
final case class LongestSeqAggr(implicit logger: Logger) extends AggregateFunction[(Int, Int), ACC, OUT] {

  def createAccumulator(): ACC = mutable.Set.empty

  def add(value: (Int, Int), acc: ACC): ACC =
    acc + value._1

  def getResult(acc: ACC): OUT = {
    logger.info("acc = {}", acc)
    var totalLC = 0
    var l, r = 0
    while (acc.nonEmpty) {
      var cur = acc.head
      acc -= cur
      r = 1
      l = 1
      while (acc.remove(cur + r)) r += 1
      while (acc.remove(cur - l)) l += 1
      if (totalLC < l + r - 1) totalLC = r + l - 1
    }
    totalLC
  }

  def merge(a: ACC, b: ACC): ACC = a ++ b
}

object LongestSeqAggr {
  type ACC = mutable.Set[Int]
  type OUT = Int

}
