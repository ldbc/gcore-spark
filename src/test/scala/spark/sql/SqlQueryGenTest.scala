package spark.sql

import algebra.expressions
import algebra.expressions._
import algebra.types.{GcoreInteger, GcoreString}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import spark.SparkSessionTestWrapper
import spark.sql.operators.SqlQueryGen

/**
  * Validates that the SQL filtering expressions built by
  * [[SqlQueryGen.expressionToSelectionPred()]] can be run through a Spark SQL query. We use a dummy
  * [[DataFrame]] table to test the expressions, whose header mimics that of a binding table's
  * (column names begin with variable name followed by a dollar sign) - this allows us to test the
  * [[PropertyRef]] expression as well.
  */
class SqlQueryGenTest extends FunSuite with BeforeAndAfterAll with SparkSessionTestWrapper {

  override def beforeAll() {
    super.beforeAll()
    table.createOrReplaceTempView("Table")
  }

  private val schema: StructType =
    StructType(List(
      StructField("v$aString", StringType, nullable = true),
      StructField("v$anInt", IntegerType, nullable = false),
      StructField("v$aBool", BooleanType, nullable = false)
    ))

  private val fooRow: Row = Row("foo", 0, true)
  private val barRow: Row = Row("bar", 1, false)
  private val bazRow: Row = Row("baz", 2, false)
  private val nullRow: Row = Row(null, 3, true)
  private val rows: Seq[Row] = Seq(fooRow, barRow, bazRow, nullRow)

  /**
    * +-----------+---------+---------+
    * | v$aString | v$anInt | v$aBool |
    * +-----------+---------+---------+
    * |   foo     |    0    |  true   |
    * +-----------+---------+---------+
    * |   bar     |    1    |  false  |
    * +-----------+---------+---------+
    * |   baz     |    2    |  false  |
    * +-----------+---------+---------+
    * |   null    |    3    |  true   |
    * +-----------+---------+---------+
    */
  private val table: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(rows), schema)

  testsFor(arithmeticExpressions())
  testsFor(conditionalExpressions())
  testsFor(mathExpressions())
  testsFor(predicateExpressions())
  testsFor(logicalExpressions())
  testsFor(unaryExpressions())

  def arithmeticExpressions(): Unit = {
    val gcoreExpressions: Seq[TestCase] =
      Seq(
        TestCase(
          "[Mul] WHERE v.anInt > 2*1",
          Gt(
            lhs = PropertyRef(Reference("v"), PropertyKey("anInt")),
            rhs = Mul(Literal(2, GcoreInteger()), Literal(1, GcoreInteger()))),
          Seq(nullRow)),
        TestCase(
          "[Div] WHERE v.anInt > 2/2",
          Gt(
            lhs = PropertyRef(Reference("v"), PropertyKey("anInt")),
            rhs = Div(Literal(2, GcoreInteger()), Literal(2, GcoreInteger()))),
          Seq(bazRow, nullRow)),
        TestCase(
          "[Mod] WHERE v.anInt > 3%2",
          Gt(
            lhs = PropertyRef(Reference("v"), PropertyKey("anInt")),
            rhs = Mod(Literal(3, GcoreInteger()), Literal(2, GcoreInteger()))),
          Seq(bazRow, nullRow)),
        TestCase(
          "[Add] WHERE v.anInt >= 1+2",
          Gte(
            lhs = PropertyRef(Reference("v"), PropertyKey("anInt")),
            rhs = Add(Literal(1, GcoreInteger()), Literal(2, GcoreInteger()))),
          Seq(nullRow)),
        TestCase(
          "[Sub] WHERE v.anInt >= 7-4",
          Gte(
            lhs = PropertyRef(Reference("v"), PropertyKey("anInt")),
            rhs = Sub(Literal(7, GcoreInteger()), Literal(4, GcoreInteger()))),
          Seq(nullRow))
      )

    gcoreExpressions foreach { testCase => testCase.runTestCase() }
  }

  def conditionalExpressions(): Unit = {
    val gcoreExpressions: Seq[TestCase] =
      Seq(
        TestCase(
          "[Eq] WHERE v.anInt = 0",
          Eq(PropertyRef(Reference("v"), PropertyKey("anInt")), Literal(0, GcoreInteger())),
          Seq(fooRow)),
        TestCase(
          "[Neq] WHERE v.anInt != 0",
          Neq(PropertyRef(Reference("v"), PropertyKey("anInt")), Literal(0, GcoreInteger())),
          Seq(barRow, bazRow, nullRow)),
        TestCase(
          "[Gt] WHERE v.anInt > 2",
          Gt(PropertyRef(Reference("v"), PropertyKey("anInt")), Literal(2, GcoreInteger())),
          Seq(nullRow)),
        TestCase(
          "[Gte] WHERE v.anInt >= 2",
          Gte(PropertyRef(Reference("v"), PropertyKey("anInt")), Literal(2, GcoreInteger())),
          Seq(bazRow, nullRow)),
        TestCase(
          "[Lt] WHERE v.anInt < 2",
          Lt(PropertyRef(Reference("v"), PropertyKey("anInt")), Literal(2, GcoreInteger())),
          Seq(fooRow, barRow)),
        TestCase(
          "[Lte] WHERE v.anInt <= 2",
          Lte(PropertyRef(Reference("v"), PropertyKey("anInt")), Literal(2, GcoreInteger())),
          Seq(fooRow, barRow, bazRow))
      )

    gcoreExpressions foreach { testCase => testCase.runTestCase() }
  }

  def mathExpressions(): Unit = {
    val gcoreExpressions: Seq[TestCase] =
      Seq(
        TestCase(
          "[Power] WHERE v.anInt > 2^1",
          Gt(
            lhs = PropertyRef(Reference("v"), PropertyKey("anInt")),
            rhs = Power(Literal(2, GcoreInteger()), Literal(1, GcoreInteger()))),
          Seq(nullRow))
      )

    gcoreExpressions foreach { testCase => testCase.runTestCase() }
  }

  def predicateExpressions(): Unit = {
    val gcoreExpressions: Seq[TestCase] =
      Seq(
        TestCase(
          "[IsNull] WHERE v.aString IS NULL",
          IsNull(PropertyRef(Reference("v"), PropertyKey("aString"))),
          Seq(nullRow)),
        TestCase(
          "[IsNotNull] WHERE v.aString IS NOT NULL",
          IsNotNull(PropertyRef(Reference("v"), PropertyKey("aString"))),
          Seq(fooRow, barRow, bazRow))
      )

    gcoreExpressions foreach { testCase => testCase.runTestCase() }
  }

  def logicalExpressions(): Unit = {
    val gcoreExpressions: Seq[TestCase] =
      Seq(
        TestCase(
          "[And] WHERE v.aString IS NOT NULL AND v.aBool = False",
          And(
            lhs = IsNotNull(PropertyRef(Reference("v"), PropertyKey("aString"))),
            rhs = Eq(PropertyRef(Reference("v"), PropertyKey("aBool")), False())),
          Seq(barRow, bazRow)),
        TestCase(
          """[Or] WHERE v.aString = "foo" OR v.anInt = 3""",
          Or(
            lhs =
              Eq(
                PropertyRef(Reference("v"), PropertyKey("aString")),
                Literal("foo", GcoreString())),
            rhs = Eq(PropertyRef(Reference("v"), PropertyKey("anInt")), Literal(3, GcoreInteger()))),
          Seq(fooRow, nullRow))
      )

    gcoreExpressions foreach { testCase => testCase.runTestCase() }
  }

  def unaryExpressions(): Unit = {
    val gcoreExpressions: Seq[TestCase] =
      Seq(
        TestCase(
          "[Minus] WHERE -v.anInt < -2 (<=> anInt > 2)",
          Lt(
            lhs = Minus(PropertyRef(Reference("v"), PropertyKey("anInt"))),
            rhs = Literal(-2, GcoreInteger())),
          Seq(nullRow)),
        TestCase(
          "[Not] WHERE NOT v.aBool",
          Not(PropertyRef(Reference("v"), PropertyKey("aBool"))),
          Seq(barRow, bazRow))
      )

    gcoreExpressions foreach { testCase => testCase.runTestCase() }
  }

  sealed case class TestCase(strExpr: String, expr: AlgebraExpression, expectedRows: Seq[Row]) {

    def runTestCase(): Unit = {
      test(s"Validate expressionToSelectionPred - $strExpr") {
        val queryExpr =
          s"SELECT * FROM Table WHERE ${SqlQueryGen.expressionToSelectionPred(expr)}"
        val actualDf = spark.sql(queryExpr)
        val expectedDf =
          spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), schema)

        compareDfs(
          actualDf.select("v$aString", "v$anInt", "v$aBool"),
          expectedDf.select("v$aString", "v$anInt", "v$aBool"))
      }
    }
  }
}
