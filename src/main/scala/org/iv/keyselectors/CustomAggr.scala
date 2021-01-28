package org.iv.keyselectors


import org.apache.flink.api.common.functions.AggregateFunction
import scala.collection.mutable

/**
 * Created by twr143 on 23.01.2021 at 21:54.
 */
import CustomAggr._

final case class CustomAggr[A]() extends AggregateFunction[A, ACC[A], OUT[A]] {

  def createAccumulator(): ACC[A] = Set.empty

  def add(value: A, acc: ACC[A]): ACC[A] = acc + value

  def getResult(acc: ACC[A]): OUT[A] = acc.toList

  def merge(a: ACC[A], b: ACC[A]): ACC[A] = a ++ b
}

object CustomAggr {
  type ACC[A] = Set[A]
  /*results, sum of coordinates equal to k, first<second*/
  type OUT[A] = List[A]

}

