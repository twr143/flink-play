import org.iv.longestseq.LongestSeqAggr
import org.scalatest.concurrent.Eventually
import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
 * Created by twr143 on 01.02.2021 at 14:16.
 */
class LongestSeqTest extends FlatSpec with Matchers with Eventually{
  implicit val logger = LoggerFactory.getLogger(getClass)
  "longestAggr" should "deliver correct result 1" in {
    val aggr = LongestSeqAggr()
     val r = aggr.getResult(mutable.Set(1,-1,2,3,5,6,7))
     r shouldBe 3
  }
  "longestAggr" should "add duplicates and after then deliver correct result" in {
    val aggr = LongestSeqAggr()
    var s = mutable.Set.empty[Int]
    aggr.add((1,1),s)
    s= aggr.add((1,1),s)
    s= aggr.add((2,2),s)
    s= aggr.add((2,2),s)
    s= aggr.add((3,3),s)
    s= aggr.add((3,3),s)
    val r = aggr.getResult(s)
     r shouldBe 3
  }

}
