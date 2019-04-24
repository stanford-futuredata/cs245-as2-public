package edu.stanford.cs245

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Literal, ScalaUDF}
import org.apache.spark.sql.catalyst.optimizer.{BooleanSimplification, ConstantFolding}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.functions._

object Tester {
  var numTests = 0
  var numPassed = 0
  var totalPoints = 0
  var totalPointsPossible = 0
  var curPoints = 0
  var assertionFailed = false

  def startTest(points: Int): Unit = {
    numTests += 1
    curPoints = points
    totalPointsPossible += points
    println(s"TEST $numTests STARTING...")
  }

  def endTest(): Unit = {
    if (!assertionFailed) {
      println(s"-----------TEST $numTests PASSED-----------")
      numPassed += 1
      totalPoints += curPoints
    } else {
      println(s"-----------TEST $numTests FAILED-----------")
    }
    assertionFailed = false
  }

  def printResults(): Unit = {
    println(s"-----------$numPassed/$numTests TESTS PASSED-----------")
  }

  def assert(cond: Boolean, msg: String): Unit = {
    if (!assertionFailed && !cond) {
      try {
        throw new RuntimeException(msg)
      } catch {
        case e: RuntimeException => e.printStackTrace()
      }
      assertionFailed = true
    }
  }

  def assert(cond: Boolean): Unit = {
    assert(cond, "")
  }

  def checkFilter(plan: LogicalPlan): Filter = {
    var filter: Filter = null
    plan transform {
      case f: Filter => {
        filter = f
        f
      }
    }
    assert(filter != null)
    filter
  }

  def checkUdf(plan: LogicalPlan, udfName: String, shouldHaveUdf: Boolean): Unit = {
    var hasUdf = false
    plan transformAllExpressions {
      case s: ScalaUDF if s.udfName.getOrElse("") == udfName => {
        hasUdf = true
        s
      }
    }
    assert(hasUdf == shouldHaveUdf,
      "Optimized logical plan should " + (if (!shouldHaveUdf) "not " else "") +
        s"have $udfName UDF")
  }

  def checkDist(plan: LogicalPlan, shouldHaveDist: Boolean): Unit = {
    checkUdf(plan, "dist", shouldHaveDist)
  }

  def checkDistSq(plan: LogicalPlan, shouldHaveDistSq: Boolean): Unit = {
    checkUdf(plan, "dist_sq", shouldHaveDistSq)
  }

  def main(args: Array[String]) {
    val spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    // definition of dist UDF
    val dist = (x1: Double, y1: Double, x2: Double, y2: Double) => {
      val xDiff = x1 - x2;
      val yDiff = y1 - y2;
      Math.sqrt(xDiff * xDiff + yDiff * yDiff)
    }
    spark.udf.register("dist", dist)
    val distUdf = udf(dist).withName("dist")

    spark.experimental.extraOptimizations = Transforms.getOptimizationPasses(spark) ++
      Seq(BooleanSimplification, ConstantFolding)

    val dataset = Seq((1.0, 1.0, 1.0, 1.0), (1.0, 1.0, 3.0, 3.0), (1.0, 1.0, 2.0, 2.0))
    val datasetNestedSeq = dataset.map(t => t.productIterator.toList)
    val df = dataset.toDF("x1", "y1", "x2", "y2")
    var query: Dataset[Row] = null
    var filter: Filter = null
    var output: Seq[Seq[Any]] = null

    // Tests
    startTest(10)
    try {
      query = df.filter("dist(x1, y1, x2, y2) < 0")
      query.explain(true)
      filter = checkFilter(query.queryExecution.optimizedPlan)
      checkDist(filter, false)
      assert(filter.condition == Literal(false, BooleanType))
      output = query.collect().map(_.toSeq).toSeq
      assert(output == Seq(), "Incorrect output: " + output)
    } catch {
      case t: Throwable => {
        assertionFailed = true
        t.printStackTrace()
      }
    }
    endTest()

    startTest(10)
    try {
      query = df.filter("dist(x1, y1, x2, y2) > 0")
      query.explain(true)
      filter = checkFilter(query.queryExecution.optimizedPlan)
      checkDistSq(filter, true)
      assert(!filter.condition.isInstanceOf[Literal])
      output = query.collect().map(_.toSeq).toSeq
      assert(output == Seq(datasetNestedSeq(1), datasetNestedSeq(2)), "Incorrect output: " + output)
    } catch {
      case t: Throwable => {
        assertionFailed = true
        t.printStackTrace()
      }
    }
    endTest()

    startTest(10)
    try {
      query = df.filter("-5 * 1 <= dist(x1, y1, x2, y2) OR x1 > 3 OR x2 > 3")
      query.explain(true)
      filter = checkFilter(query.queryExecution.optimizedPlan)
      checkDist(filter, false)
      assert(filter.condition == Literal(true, BooleanType))
      output = query.collect().map(_.toSeq).toSeq
      assert(output == datasetNestedSeq, "Incorrect output: " + output)
    } catch {
      case t: Throwable => {
        assertionFailed = true
        t.printStackTrace()
      }
    }
    endTest()

    startTest(10)
    try {
      query = df.filter("x2 > 0").selectExpr("-4 >= dist(x1, y1, x2, y2)")
      query.explain(true)
      checkDist(query.queryExecution.optimizedPlan, false)
      checkDistSq(query.queryExecution.optimizedPlan, false)
      output = query.collect().map(_.toSeq).toSeq
      assert(output == Seq(Seq(false), Seq(false), Seq(false)), "Incorrect output: " + output)
    } catch {
      case t: Throwable => {
        assertionFailed = true
        t.printStackTrace()
      }
    }
    endTest()

    startTest(10)
    try {
      query = df.filter("x2 > 1").selectExpr("dist(x1, y1, x2, y2) == -1 * 1")
      query.explain(true)
      checkDist(query.queryExecution.optimizedPlan, false)
      checkDistSq(query.queryExecution.optimizedPlan, false)
      output = query.collect().map(_.toSeq).toSeq
      assert(output == Seq(Seq(false), Seq(false)), "Incorrect output: " + output)
    } catch {
      case t: Throwable => {
        assertionFailed = true
        t.printStackTrace()
      }
    }
    endTest()

    startTest(10)
    try {
      query = df.filter("dist(x1, y1, x2, y2) == 0")
      query.explain(true)
      filter = checkFilter(query.queryExecution.optimizedPlan)
      checkDist(filter, false)
      checkDistSq(filter, false)
      assert(!filter.condition.isInstanceOf[Literal])
      output = query.collect().map(_.toSeq).toSeq
      assert(output == Seq(datasetNestedSeq(0)), "Incorrect output: " + output)
    } catch {
      case t: Throwable => {
        assertionFailed = true
        t.printStackTrace()
      }
    }
    endTest()

    startTest(10)
    try {
      query = df.filter("x1 < 3 AND y1 < 3 AND dist(x1, y1, x2, y2) == 0")
      query.explain(true)
      filter = checkFilter(query.queryExecution.optimizedPlan)
      checkDist(filter, false)
      checkDistSq(filter, false)
      assert(!filter.condition.isInstanceOf[Literal])
      output = query.collect().map(_.toSeq).toSeq
      assert(output == Seq(datasetNestedSeq(0)), "Incorrect output: " + output)
    } catch {
      case t: Throwable => {
        assertionFailed = true
        t.printStackTrace()
      }
    }
    endTest()

    startTest(10)
    try {
      query = df.filter("dist(x1, y1, x2, y2) > 2")
      query.explain(true)
      filter = checkFilter(query.queryExecution.optimizedPlan)
      checkDist(filter, false)
      checkDistSq(filter, true)
      output = query.collect().map(_.toSeq).toSeq
      assert(output == Seq(datasetNestedSeq(1)), "Incorrect output: " + output)
    } catch {
      case t: Throwable => {
        assertionFailed = true
        t.printStackTrace()
      }
    }
    endTest()

    startTest(10)
    try {
      query = df.filter("x2 > 2 AND dist(x1, y1, x2, y2) > dist(x1, y1, 1, 1) AND y2 > 2")
      query.explain(true)
      filter = checkFilter(query.queryExecution.optimizedPlan)
      checkDist(filter, false)
      checkDistSq(filter, true)
      output = query.collect().map(_.toSeq).toSeq
      assert(output == Seq(datasetNestedSeq(1)), "Incorrect output: " + output)
    } catch {
      case t: Throwable => {
        assertionFailed = true
        t.printStackTrace()
      }
    }
    endTest()

    startTest(10)
    try {
      query = df.filter("dist(x1, y1, x2, y2) > rand(0) + 1")
      query.explain(true)
      filter = checkFilter(query.queryExecution.optimizedPlan)
      checkDist(filter, true)
      checkDistSq(filter, false)
    } catch {
      case t: Throwable => {
        assertionFailed = true
        t.printStackTrace()
      }
    }
    endTest()

    startTest(20)
    try {
      query = df.sort(distUdf('x1, 'y1, 'x2, 'y2))
      query.explain(true)
      checkDist(query.queryExecution.optimizedPlan, false)
      checkDistSq(query.queryExecution.optimizedPlan, true)
      output = query.collect().map(_.toSeq).toSeq
      assert(output == Seq(datasetNestedSeq(0), datasetNestedSeq(2), datasetNestedSeq(1)),
        "Incorrect output: " + output)
    } catch {
      case t: Throwable => {
        assertionFailed = true
        t.printStackTrace()
      }
    }
    endTest()

    startTest(20)
    try {
      query = df.sort(distUdf('x2, 'y2, lit(3.0), lit(3.0)) + 'x2)
      query.explain(true)
      checkDist(query.queryExecution.optimizedPlan, true)
      checkDistSq(query.queryExecution.optimizedPlan, false)
      output = query.collect().map(_.toSeq).toSeq
      assert(output == Seq(datasetNestedSeq(1), datasetNestedSeq(2), datasetNestedSeq(0)),
        "Incorrect output: " + output)
    } catch {
      case t: Throwable => {
        assertionFailed = true
        t.printStackTrace()
      }
    }
    endTest()

    spark.stop()
    printResults()
    println("MAXIMUM POSSIBLE SCORE: " + totalPointsPossible)
    println("SCORE: " + totalPoints)
  }
}
