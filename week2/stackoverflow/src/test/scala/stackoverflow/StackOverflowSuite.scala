package stackoverflow

import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  def initStackOverflow(): Boolean = {
    try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
  }

  test("test groupedPostings") {
    assert(initStackOverflow(), "Can't instantiate a StackOverflow object")
    import StackOverflow._

    val postings = List(
      Posting(1, 1, Option.empty, Option.empty, 2, Option("Scala")),
      Posting(1, 2, Option.empty, Option.empty, 9, Option("Java")),
      Posting(2, 3, Option.empty, Option(1), 3, Option.empty),
      Posting(2, 4, Option.empty, Option(2), 10, Option.empty)
    )
    val postingsRDD:RDD[Posting] = sc.parallelize(postings)

    val groupedByQID = testObject.groupedPostings(postingsRDD).collect()
//    expected
//    1, [(Posting(1, 1, Option.empty, Option.empty, 2, Option("Scala")), Posting(2, 3, Option.empty, Option(1), 3, Option.empty))]
//    2, [(Posting(1, 2, Option.empty, Option.empty, 9, Option("Java")), Posting(2, 4, Option.empty, Option(2), 10, Option.empty))]
    assert(groupedByQID.length == 2, "Invalid number of groups")
    assert(groupedByQID.head._1 == 1, "Invalid QID for 1st element")
    assert(groupedByQID.head._2.size == 2, "Missing either question or answer in 1st group")
    assert(groupedByQID.head._2.head._2.id == 3, "Invalid Id in answer for question 1")
    assert(groupedByQID.head._2.head._2.parentId.get == 1, "Invalid parentId in answer for question 1")
  }

}
