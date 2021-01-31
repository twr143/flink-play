package org.iv.aggregate

import org.apache.flink.api.common.functions.AggregateFunction

import MaxComposedAggr._


/**
 * Created by twr143 on 23.01.2021 at 21:54.
 */
// another job is better
final case class MaxComposedAggr(k: Int) extends AggregateFunction[(Int, Int), ACC, OUT] {
  implicit val ord = Ordering.fromLessThan[(Int, Int)]({ case (f, s) => if (f._2 != s._2) f._2 > s._2 else f._1 < s._1 })

  def createAccumulator(): ACC = (Set.empty, Set.empty)

  def add(value: (Int, Int), acc: ACC): ACC = {
    if (acc._1.contains(k - value._1)) {
      (acc._1 - (k - value._1), acc._2 + (if (k - value._1 <= value._1) (k - value._1, value._1) else (value._1, k - value._1)))
    } else 
      (acc._1 + value._1, acc._2)
  }

  def getResult(accumulator: ACC): OUT = {
    accumulator._2.toList
  }

  def merge(a: ACC, b: ACC): ACC = (a._1 ++ b._1, a._2 ++ b._2)
}

object MaxComposedAggr {
  type ACC = (Set[Int]/*keyed set*/, Set[(Int, Int)]/*results, sum of coordinates equal to k, first<second*/)
  type OUT = List[(Int, Int)]

}
