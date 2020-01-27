/*
 * gcore-spark is the reference implementation of the G-CORE graph query
 * language by the Linked Data Benchmark Council (LDBC) - ldbcouncil.org
 *
 * The copyrights of the source code in this file belong to:
 * - CWI (www.cwi.nl), 2017-2018
 *
 * This software is released in open source under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package spark.sql

import algebra.expressions._
import algebra.target_api.TargetTreeNode
import algebra.types.GraphPattern
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import spark.SparkSessionTestWrapper
import spark.sql.SqlQuery.{expandExpression, expressionToSelectionPred}
import spark.sql.operators.SqlBindingTableMetadata

/**
  * Validates that the SQL filtering expressions built by [[SqlQuery.expandExpression]] can be run
  * through a Spark SQL query. We use a dummy [[DataFrame]] table to test the expressions, whose
  * header mimics that of a binding table's (column names begin with variable name followed by a
  * dollar sign) - this allows us to test the [[PropertyRef]] expression as well.
  */
@RunWith(classOf[JUnitRunner])
class SqlQueryTest extends FunSuite with BeforeAndAfterAll with SparkSessionTestWrapper {

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
  testsFor(aggregateExpressions())
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

  def aggregateExpressions(): Unit = {
    val gcoreExpressions: Seq[AggTestCase] =
      Seq(
        AggTestCase(
          "[Collect] group_by(v.aBool), collect(v.aString)",
          Collect(distinct = false, PropertyRef(Reference("v"), PropertyKey("aString"))),
          Seq(/*true, null omitted =>*/ Seq("foo"), /*false => */ Seq("bar", "baz"))),
        AggTestCase(
          "[Count] group_by(v.aBool), count(*)",
          Count(distinct = false, Star),
          Seq(/*true, null counted =>*/ 2, /*false => */ 2)),
        AggTestCase(
          "[Count distinct] group_by(v.aBool), count(distinct v.anInt)",
          Count(distinct = true, PropertyRef(Reference("v"), PropertyKey("anInt"))),
          Seq(/*true =>*/ 2, /*false => */ 2)),
        AggTestCase(
          "[Min] group_by(v.aBool), min(v.anInt)",
          Min(distinct = false, PropertyRef(Reference("v"), PropertyKey("anInt"))),
          Seq(/*true =>*/ 0, /*false => */ 1)),
        AggTestCase(
          "[Max] group_by(v.aBool), max(v.anInt)",
          Max(distinct = false, PropertyRef(Reference("v"), PropertyKey("anInt"))),
          Seq(/*true =>*/ 3, /*false => */ 2)),
        AggTestCase(
          "[Sum] group_by(v.aBool), sum(v.anInt)",
          Sum(distinct = false, PropertyRef(Reference("v"), PropertyKey("anInt"))),
          Seq(/*true =>*/ 3, /*false => */ 3)),
        AggTestCase(
          "[Avg] group_by(v.aBool), avg(v.anInt)",
          Avg(distinct = false, PropertyRef(Reference("v"), PropertyKey("anInt"))),
          Seq(/*true =>*/ 1.5, /*false => */ 1.5)),
        AggTestCase(
          "[GroupConcat] group_by(v.aBool), group_concat(v.aString)",
          GroupConcat(distinct = false, PropertyRef(Reference("v"), PropertyKey("aString"))),
          Seq(/*true =>*/ "foo", /*false => */ "bar,baz")),
        AggTestCase(
          "[GroupConcat distinct] group_by(v.aBool), group_concat(distinct v.aString)",
          GroupConcat(distinct = true, PropertyRef(Reference("v"), PropertyKey("aString"))),
          Seq(/*true =>*/ "foo", /*false => */ "bar,baz"))
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
      override val bindingTable: SqlBindingTableMetadata =
        SqlBindingTableMetadata(
          sparkSchemaMap = Map(Reference("v") -> vschema),
          sparkBtableSchema = vschema,
          btableOps = SqlQuery(resQuery = vselect)
        )
    }

    val vrestrictedScan = new TargetTreeNode {
      override val bindingTable: SqlBindingTableMetadata =
        SqlBindingTableMetadata(
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
        val exprStr = expressionToSelectionPred(expr, Map(Reference("v") -> vschema), "v")
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

  sealed case class AggTestCase(strExpr: String, expr: AlgebraExpression, expected: Seq[Any]) {

    def runTestCase(): Unit = {
      test(s"Validate expressionToSelectionPred - $strExpr") {
        val exprStr = expandExpression(expr)
        val queryExpr = s"SELECT $exprStr AS resCol FROM vTable v GROUP BY `v$$aBool`"
        val actual = spark.sql(queryExpr).collect().map(_(0))

        assert(actual.length == expected.size)
        assert(actual.toSet == expected.toSet)
      }
    }
  }
}
