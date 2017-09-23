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
    assert(groupedByQID.head._2.size == 1, "Missing either question or answer in 1st group")
    assert(groupedByQID.head._2.head._2.id == 3, "Invalid Id in answer for question 1")
    assert(groupedByQID.head._2.head._2.parentId.get == 1, "Invalid parentId in answer for question 1")
  }

  test("test scoredPostings") {
    assert(initStackOverflow(), "Can't instantiate a StackOverflow object")
    import StackOverflow._
    val grouped = List(
      (1, List(
        (Posting(1, 1, Option.empty, Option.empty, 2, Option("CSS")), Posting(2, 3, Option.empty, Option(1), 3, Option.empty)),
        (Posting(1, 1, Option.empty, Option.empty, 2, Option("CSS")), Posting(2, 4, Option.empty, Option(1), 67, Option.empty))
      )),
      (2, List(
        (Posting(1, 2, Option.empty, Option.empty, 9, Option("PHP")), Posting(2, 5, Option.empty, Option(2), 89, Option.empty)),
        (Posting(1, 2, Option.empty, Option.empty, 9, Option("PHP")), Posting(2, 6, Option.empty, Option(2), 10, Option.empty))
      )),
      (3, List(
        (Posting(1, 3, Option.empty, Option.empty, 4, Option("Ruby")), Posting(2, 7, Option.empty, Option(2), 3, Option.empty))
      )),
      (4, List(
        (Posting(1, 4, Option.empty, Option.empty, 10, Option("Java")), Posting(2, 8, Option.empty, Option(2), 10, Option.empty)),
        (Posting(1, 4, Option.empty, Option.empty, 10, Option("Java")), Posting(2, 9, Option.empty, Option(2), 30, Option.empty)),
        (Posting(1, 4, Option.empty, Option.empty, 10, Option("Java")), Posting(2, 10, Option.empty, Option(2), 15, Option.empty))
      ))
    )
    val scored = testObject.scoredPostings(sc.parallelize(grouped)).collect()
//    expected
//    ((1, 6,   None, None, 140, Some(CSS)),  67)
//    ((1, 42,  None, None, 155, Some(PHP)),  89)
//    ((1, 72,  None, None, 16,  Some(Ruby)), 3)
//    ((1, 126, None, None, 33,  Some(Java)), 30)
    assert(scored.length == 4, "Invalid number of scored questions")
    val rubyLang = scored.find { case (question, highScore) => question.tags.contains("Ruby") }.get
    assert(rubyLang._1.id == 3, "Invalid Ruby scored element id")
    assert(rubyLang._2 == 3, "Invalid Ruby element score")
    val javaLang = scored.find { case (question, highScore) => question.tags.contains("Java") }.get
    assert(javaLang._1.id == 4, "Invalid last scored element id")
    assert(javaLang._2 == 30, "Invalid last element score")
  }

  test("test vectorPostings") {
    assert(initStackOverflow(), "Can't instantiate a StackOverflow object")
    import StackOverflow._

    val scored = List(
      (Posting(1, 6,   None, None, 140, Some("CSS")),  67),
      (Posting(1, 42,  None, None, 155, Some("PHP")),  89),
      (Posting(1, 72,  None, None, 16,  Some("Ruby")), 3),
      (Posting(1, 126, None, None, 33,  Some("Java")), 30)
    )

    val vectors = testObject.vectorPostings(sc.parallelize(scored)).collect()
//    CSS (8 * 50000, 67)
//    PHP (3 * 50000, 89)
//    Ruby (7 * 50000, 3)
//    Java (2 * 50000, 30)
    assert(vectors.head._1 == 7 * 50000)
    assert(vectors.head._2 == 67)
    assert(vectors.last._1 == 1 * 50000)
    assert(vectors.last._2 == 30)
  }
}
