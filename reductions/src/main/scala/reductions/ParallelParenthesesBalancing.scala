package reductions

import scala.annotation._
import org.scalameter._
import common._

object ParallelParenthesesBalancingRunner {

  @volatile var seqResult = false

  @volatile var parResult = false

  val standardConfig = config(
    Key.exec.minWarmupRuns -> 40,
    Key.exec.maxWarmupRuns -> 80,
    Key.exec.benchRuns -> 120,
    Key.verbose -> true
  ) withWarmer(new Warmer.Default)

  def main(args: Array[String]): Unit = {
    val length = 100000000
    val chars = new Array[Char](length)
    val threshold = 10000
    val seqtime = standardConfig measure {
      seqResult = ParallelParenthesesBalancing.balance(chars)
    }
    println(s"sequential result = $seqResult")
    println(s"sequential balancing time: $seqtime ms")

    val fjtime = standardConfig measure {
      parResult = ParallelParenthesesBalancing.parBalance(chars, threshold)
    }
    println(s"parallel result = $parResult")
    println(s"parallel balancing time: $fjtime ms")
    println(s"speedup: ${seqtime / fjtime}")
  }
}

object ParallelParenthesesBalancing {

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def balance(chars: Array[Char]): Boolean = {
    def balanceParentheses(c: Array[Char], parcount: Int) : Boolean = {
      if (c.isEmpty) parcount == 0
      else if (c.head == '(') balanceParentheses(c.tail, parcount + 1)
      else if (c.head == ')') if (parcount == 0) false else balanceParentheses(c.tail, parcount - 1)
      else balanceParentheses(c.tail, parcount)
    }
    balanceParentheses(chars, 0)
  }

  /** Returns `true` iff the parentheses in the input `chars` are balanced.
   */
  def parBalance(chars: Array[Char], threshold: Int): Boolean = {

    def traverse(idx: Int, until: Int, closingParenAtStart: Int, parCount: Int) : (Int, Int) = {
      if (idx == until) (closingParenAtStart, parCount)
      else {
        chars(idx) match {
          case ')' => if (parCount == 0) traverse(idx+1, until, closingParenAtStart+1, parCount) else
                      traverse(idx+1, until, closingParenAtStart, parCount - 1)
          case '(' => traverse(idx+1, until, closingParenAtStart, parCount + 1)
          case _ => traverse(idx+1, until, closingParenAtStart, parCount)
        }
      }
    }

    def reduce(from: Int, until: Int) : (Int, Int) = {
      if (until - from <= threshold) traverse(from, until, 0, 0)
      else {
        val mid = (until + from)/2
        val (left, right) = parallel(reduce(from, mid), reduce(mid, until))
        (left._1, (left._2 - right._1 + right._2))
      }
    }

    reduce(0, chars.length) == (0, 0)
  }

  // For those who want more:
  // Prove that your reduction operator is associative!

}