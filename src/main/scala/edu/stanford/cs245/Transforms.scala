package edu.stanford.cs245

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, BinaryComparison, EqualTo, Expression, GreaterThan,
  GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, Multiply, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{BooleanType, DoubleType}

object Transforms {

  // check if a ScalaUDF Expression is our dist UDF
  def isDistUdf(udf: ScalaUDF): Boolean = {
    udf.udfName.getOrElse("") == "dist"
  }

  // get an Expression representing the dist_sq UDF with the provided
  // arguments
  def getDistSqUdf(args: Seq[Expression]): ScalaUDF = {
    ScalaUDF(
      (x1: Double, y1: Double, x2: Double, y2: Double) => {
        val xDiff = x1 - x2;
        val yDiff = y1 - y2;
        xDiff * xDiff + yDiff * yDiff
      }, DoubleType, args, Seq(DoubleType, DoubleType, DoubleType, DoubleType),
      udfName = Option.apply("dist_sq"))
  }

  // Return any additional optimization passes here
  def getOptimizationPasses(spark: SparkSession): Seq[Rule[LogicalPlan]] = {
    Seq(EliminateZeroDists(spark))
  }

  case class EliminateZeroDists(spark: SparkSession) extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
      case udf: ScalaUDF if isDistUdf(udf) && udf.children(0) == udf.children(2) &&
        udf.children(1) == udf.children(3) => Literal(0.0, DoubleType)
    }
  }
}
