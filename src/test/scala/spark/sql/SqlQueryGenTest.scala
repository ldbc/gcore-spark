package spark.sql

import algebra.expressions._
import algebra.types.{GcoreInteger, GcoreString, GraphPattern}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import planner.operators.VertexScan
import planner.target_api.BindingTable
import planner.trees.TargetTreeNode
import spark.SparkSessionTestWrapper
import spark.sql.operators.{SparkBindingTable, SparkVertexScan, SqlQuery, SqlQueryGen}

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
    vtable.createOrReplaceTempView("vTable")
    wtable.createOrReplaceTempView("wTable")
  }

  private val vschema: StructType =
    StructType(List(
      StructField("v$id", IntegerType, nullable = false),
      StructField("v$aString", StringType, nullable = true),
      StructField("v$anInt", IntegerType, nullable = false),
      StructField("v$aBool", BooleanType, nullable = false)
    ))

  private val wschema: StructType =
    StructType(List(
      StructField("v$id", IntegerType, nullable = false),
      StructField("w$dummyCol", StringType, nullable = true)
    ))

  private val fooRow: Row = Row(0, "foo", 0, true)
  private val barRow: Row = Row(1, "bar", 1, false)
  private val bazRow: Row = Row(2, "baz", 2, false)
  private val nullRow: Row = Row(3, null, 3, true)
  private val vrows: Seq[Row] = Seq(fooRow, barRow, bazRow, nullRow)

  private val dummyRow: Row = Row(0, "dummy")
  private val wrows: Seq[Row] = Seq(dummyRow)

  /**
    * +------+-----------+---------+---------+
    * | v$id | v$aString | v$anInt | v$aBool |
    * +------+-----------+---------+---------+
    * |   0  |   foo     |    0    |  true   |
    * +------+-----------+---------+---------+
    * |   1  |   bar     |    1    |  false  |
    * +------+-----------+---------+---------+
    * |   2  |   baz     |    2    |  false  |
    * +------+-----------+---------+---------+
    * |   3  |   null    |    3    |  true   |
    * +------+-----------+---------+---------+
    */
  private val vtable: DataFrame =
    spark.createDataFrame(spark.sparkContext.parallelize(vrows), vschema)

  /**
    * +------+------------+
    * | w$id | w$dummyCol |
    * +------+------------+
    * |   0  |   dummy    |
    * +------+------------+
    */
  private val wtable: DataFrame=
    spark.createDataFrame(spark.sparkContext.parallelize(wrows), wschema)

  testsFor(arithmeticExpressions())
  testsFor(conditionalExpressions())
  testsFor(mathExpressions())
  testsFor(predicateExpressions())
  testsFor(logicalExpressions())
  testsFor(unaryExpressions())
  testsFor(exists())

  def arithmeticExpressions(): Unit = {
    val gcoreExpressions: Seq[TestCase] =
      Seq(
        TestCase(
          "[Mul] WHERE v.anInt > 2*1",
          Gt(
            lhs = PropertyRef(Reference("v"), PropertyKey("anInt")),
            rhs = Mul(IntLiteral(2), IntLiteral(1))),
          Seq(nullRow)),
        TestCase(
          "[Div] WHERE v.anInt > 2/2",
          Gt(
            lhs = PropertyRef(Reference("v"), PropertyKey("anInt")),
            rhs = Div(IntLiteral(2), IntLiteral(2))),
          Seq(bazRow, nullRow)),
        TestCase(
          "[Mod] WHERE v.anInt > 3%2",
          Gt(
            lhs = PropertyRef(Reference("v"), PropertyKey("anInt")),
            rhs = Mod(IntLiteral(3), IntLiteral(2))),
          Seq(bazRow, nullRow)),
        TestCase(
          "[Add] WHERE v.anInt >= 1+2",
          Gte(
            lhs = PropertyRef(Reference("v"), PropertyKey("anInt")),
            rhs = Add(IntLiteral(1), IntLiteral(2))),
          Seq(nullRow)),
        TestCase(
          "[Sub] WHERE v.anInt >= 7-4",
          Gte(
            lhs = PropertyRef(Reference("v"), PropertyKey("anInt")),
            rhs = Sub(IntLiteral(7), IntLiteral(4))),
          Seq(nullRow))
      )

    gcoreExpressions foreach { testCase => testCase.runTestCase() }
  }

  def conditionalExpressions(): Unit = {
    val gcoreExpressions: Seq[TestCase] =
      Seq(
        TestCase(
          "[Eq] WHERE v.anInt = 0",
          Eq(PropertyRef(Reference("v"), PropertyKey("anInt")), IntLiteral(0)),
          Seq(fooRow)),
        TestCase(
          "[Neq] WHERE v.anInt != 0",
          Neq(PropertyRef(Reference("v"), PropertyKey("anInt")), IntLiteral(0)),
          Seq(barRow, bazRow, nullRow)),
        TestCase(
          "[Gt] WHERE v.anInt > 2",
          Gt(PropertyRef(Reference("v"), PropertyKey("anInt")), IntLiteral(2)),
          Seq(nullRow)),
        TestCase(
          "[Gte] WHERE v.anInt >= 2",
          Gte(PropertyRef(Reference("v"), PropertyKey("anInt")), IntLiteral(2)),
          Seq(bazRow, nullRow)),
        TestCase(
          "[Lt] WHERE v.anInt < 2",
          Lt(PropertyRef(Reference("v"), PropertyKey("anInt")), IntLiteral(2)),
          Seq(fooRow, barRow)),
        TestCase(
          "[Lte] WHERE v.anInt <= 2",
          Lte(PropertyRef(Reference("v"), PropertyKey("anInt")), IntLiteral(2)),
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
            rhs = Power(IntLiteral(2), IntLiteral(1))),
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
            rhs = Eq(PropertyRef(Reference("v"), PropertyKey("aBool")), False)),
          Seq(barRow, bazRow)),
        TestCase(
          """[Or] WHERE v.aString = "foo" OR v.anInt = 3""",
          Or(
            lhs =
              Eq(
                PropertyRef(Reference("v"), PropertyKey("aString")),
                StringLiteral("foo")),
            rhs =
              Eq(PropertyRef(Reference("v"), PropertyKey("anInt")), IntLiteral(3))),
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
            rhs = IntLiteral(-2)),
          Seq(nullRow)),
        TestCase(
          "[Not] WHERE NOT v.aBool",
          Not(PropertyRef(Reference("v"), PropertyKey("aBool"))),
          Seq(barRow, bazRow))
      )

    gcoreExpressions foreach { testCase => testCase.runTestCase() }
  }

  def exists(): Unit = {
    val vselect = "SELECT * FROM vTable" // WHERE EXISTS (v)
    val vrestricted = "SELECT * FROM vTable WHERE `v$id` = 0" // WHERE EXISTS (v)
    val wselect = "SELECT * FROM wTable" // WHERE EXISTS (w)

    val vscan = new TargetTreeNode {
      override val bindingTable: BindingTable =
        SparkBindingTable(
          sparkSchemaMap = Map(Reference("v") -> vschema),
          sparkBtableSchema = vschema,
          btableOps = SqlQuery(resQuery = vselect)
        )
    }

    val vrestrictedScan = new TargetTreeNode {
      override val bindingTable: BindingTable =
        SparkBindingTable(
          sparkSchemaMap = Map(Reference("v") -> vschema),
          sparkBtableSchema = vschema,
          btableOps = SqlQuery(resQuery = vrestricted)
        )
    }

    val vexists = Exists(GraphPattern(Seq.empty))
    vexists.children = Seq(vscan)

    val vrestrictedExists = Exists(GraphPattern(Seq.empty))
    vrestrictedExists.children = Seq(vrestrictedScan)

    val gcoreExpressions: Seq[TestCase] =
      Seq(
        TestCase(
          "[Exists] WHERE EXISTS (v) (common binding v)",
          vexists,
          Seq(fooRow, barRow, bazRow, nullRow)),
        TestCase(
          "[Not Exists] WHERE NOT EXISTS (v) (common binding v)",
          Not(vexists),
          Seq.empty),
        TestCase(
          "[Exists] WHERE NOT EXISTS (v) (sub-table of vTable)",
          vrestrictedExists,
          Seq(fooRow))
      )

    gcoreExpressions foreach { testCase => testCase.runTestCase() }
  }

  sealed case class TestCase(strExpr: String, expr: AlgebraExpression, expectedRows: Seq[Row]) {

    def runTestCase(): Unit = {
      test(s"Validate expressionToSelectionPred - $strExpr") {
        val exprStr =
          SqlQueryGen.expressionToSelectionPred(expr, Map(Reference("v") -> vschema), "v")
        val queryExpr = s"SELECT * FROM vTable v WHERE $exprStr"
        val actualDf = spark.sql(queryExpr)
        val expectedDf =
          spark.createDataFrame(spark.sparkContext.parallelize(expectedRows), vschema)

        compareDfs(
          actualDf.select("v$id", "v$aString", "v$anInt", "v$aBool"),
          expectedDf.select("v$id", "v$aString", "v$anInt", "v$aBool"))
      }
    }
  }
}
