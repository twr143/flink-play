package org.iv.keyselectors


import org.apache.flink.api.common.functions.AggregateFunction
import scala.collection.mutable

/**
 * Created by twr143 on 23.01.2021 at 21:54.
 */
import CustomAggr._

final case class CustomAggr() extends AggregateFunction[A, ACC, OUT] {
  implicit val ord = Ordering.fromLessThan[(String, Int)]({ case (f, s) => if (f._2 != s._2) f._2 > s._2 else f._1 < s._1 })

  def createAccumulator(): ACC = Set.empty

  def add(value: A, acc: ACC): ACC = acc.filter( _._1.v != value._1.v) + value

  def getResult(acc: ACC): OUT = acc.toList

  def merge(a: ACC, b: ACC): ACC = a ++ b
}

object CustomAggr {
  type A = (Value, Int)
  type ACC = Set[A]
  /*results, sum of coordinates equal to k, first<second*/
  type OUT = List[A]

}

