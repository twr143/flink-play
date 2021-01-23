package org.iv.aggregate

import org.apache.flink.api.common.functions.AggregateFunction
import scala.collection.mutable

/**
 * Created by twr143 on 23.01.2021 at 21:54.
 */
final case class MaxNAggr() extends AggregateFunction[(String, Int), mutable.TreeSet[(String, Int)], List[(String, Int)]] {
  implicit val ord = Ordering.fromLessThan[(String, Int)]({ case (f, s) => if (f._2 != s._2) f._2 > s._2 else f._1 < s._1 })
  def createAccumulator(): mutable.TreeSet[(String, Int)] = mutable.TreeSet.empty

  def add(value: (String, Int), accumulator: mutable.TreeSet[(String, Int)]): mutable.TreeSet[(String, Int)] = {
    (accumulator.filter(_._1 != value._1) += value).take(5)
  }

  def getResult(accumulator: mutable.TreeSet[(String, Int)]): List[(String, Int)] = {
    accumulator.toList
  }

  def merge(a: mutable.TreeSet[(String, Int)], b: mutable.TreeSet[(String, Int)]): mutable.TreeSet[(String, Int)] = a ++ b
}
