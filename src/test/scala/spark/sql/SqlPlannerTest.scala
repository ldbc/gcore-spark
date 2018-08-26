package spark.sql

import algebra.AlgebraRewriter
import algebra.expressions.Label
import algebra.operators.Column._
import algebra.operators._
import algebra.trees.{AlgebraContext, AlgebraTreeNode}
import compiler.CompileContext
import org.apache.spark.sql.functions.{avg, expr, first, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FunSuite}
import parser.SpoofaxParser
import parser.trees.ParseContext
import schema.Catalog.{START_BASE_TABLE_INDEX, TABLE_INDEX_INCREMENT}
import schema.EntitySchema.LabelRestrictionMap
import schema.{Catalog, SchemaMap, Table}
import spark._
import spark.sql.operators.EntityConstruct

/**
  * Verifies that the [[SqlPlanner]] creates correct [[DataFrame]] binding tables and constructs the
  * expected [[SparkGraph]]s. The tests also assert that the implementation of the physical
  * operators with Spark produces the expected results. The operators are not tested individually -
  * we only look at the results obtained by running the code produced by these operators.
  */
class SqlPlannerTest extends FunSuite
  with TestGraph with BeforeAndAfterEach with BeforeAndAfterAll with SparkSessionTestWrapper {

  import spark.implicits._

  val db: SparkCatalog = catalog(spark)
  val graph: SparkGraph = catsGraph(spark)
  val sparkPlanner: SqlPlanner = SqlPlanner(CompileContext(db, spark))

  val parser: SpoofaxParser = SpoofaxParser(ParseContext(db))
  val algebraRewriter: AlgebraRewriter = AlgebraRewriter(AlgebraContext(db))

  val idCol: String = ID_COL.columnName
  val fromIdCol: String = FROM_ID_COL.columnName
  val toIdCol: String = TO_ID_COL.columnName
  val labelCol: String = TABLE_LABEL_COL.columnName
  val edgesCol: String = EDGE_SEQ_COL.columnName

  /** MATCH (c:Cat)-[e:Eats]->(f:Food) */
  val bindingTableSchema: StructType =
    StructType(List(
      StructField(s"c$$$idCol", IntegerType, nullable = false),
      StructField(s"c$$$labelCol", StringType, nullable = false),
      StructField("c$name", StringType, nullable = false),
      StructField("c$age", DoubleType, nullable = false),
      StructField("c$weight", IntegerType, nullable = false),
      StructField("c$onDiet", BooleanType, nullable = false),

      StructField(s"e$$$idCol", IntegerType, nullable = false),
      StructField(s"e$$$fromIdCol", IntegerType, nullable = false),
      StructField(s"e$$$toIdCol", IntegerType, nullable = false),
      StructField(s"e$$$labelCol", StringType, nullable = false),
      StructField("e$gramsPerDay", DoubleType, nullable = false),

      StructField(s"f$$$idCol", IntegerType, nullable = false),
      StructField(s"f$$$labelCol", StringType, nullable = false),
      StructField("f$brand", StringType, nullable = false)
    ))
  val bindingTableData: Seq[CatEatsFood] =
    createCatEatsFoodData(
      Seq(
        (coby, cobyEatsPurina, purina), (hosico, hosicoEatsGourmand, gourmand),
        (maru, maruEatsWhiskas, whiskas), (grumpy, grumpyEatsGourmand, gourmand)))
  val bindingTableRows: Seq[Row] =
    bindingTableData.map {
      case CatEatsFood(
      catId, name, age, weight, onDiet, foodId, brand, eatsId, gramsPerDay, fromId, toId) =>
        Row(
          catId, "Cat", name, age, weight, onDiet,
          eatsId, fromId, toId, "Eats", gramsPerDay,
          foodId, "Food", brand)
    }
  val bindingTable: DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(bindingTableRows),
      bindingTableSchema)

  /**
    * btable1 = SELECT c1 = c, f1 = f, e1 = e WHERE c IN {101, 102}
    * btable2 = SELECT c2 = c, f2 = f, e2 = e WHERE c IN {103, 104}
    *
    *                               +-----+-----+-----+-----+-----+-----+
    *                               |c1$id|f1$id|e1$id|c2$id|f2$id|e2$id|
    *                               +-----+-----+-----+-----+-----+-----+
    *                               |  101|  105|  201|  103|  106|  203|
    * btable1 CROSS JOIN btable2 => |  101|  105|  201|  104|  107|  204|
    *                               |  102|  107|  202|  103|  106|  203|
    *                               |  102|  107|  202|  104|  107|  204|
    *                               +-----+-----+-----+-----+-----+-----+
    */
  val bindingTableDuplicateData: DataFrame = {
    val btable1: DataFrame =
      bindingTable
        .select(s"c$$$idCol", s"f$$$idCol", s"e$$$idCol")
        .where(s"`c$$$idCol` IN (101, 102)")
        .toDF(s"c1$$$idCol", s"f1$$$idCol", s"e1$$$idCol")
    val btable2: DataFrame =
      bindingTable
        .select(s"c$$$idCol", s"f$$$idCol", s"e$$$idCol")
        .where(s"`c$$$idCol` IN (103, 104)")
        .toDF(s"c2$$$idCol", s"f2$$$idCol", s"e2$$$idCol")
    btable1.crossJoin(btable2)
  }

  // For now, we are not checking graph name, so we assign a random one.
  val FOO_GRAPH_NAME: String = "foo"

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    Catalog.resetBaseEntityTableIndex()
  }

  override def beforeAll() {
    super.beforeAll()
    db.registerGraph(graph)
    db.setDefaultGraph("cats graph")
  }

  /************************************ CONSTRUCT *************************************************/
  test("VertexCreate of bound variable - CONSTRUCT (c) MATCH (c)-[e]->(f)") {
    val vertex = extractConstructClauses("CONSTRUCT (c) MATCH (c)")
    val actualGraph = sparkPlanner.constructGraph(bindingTable, vertex)
    val expectedGraph = new SparkGraph {
      override def graphName: String = FOO_GRAPH_NAME

      override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def edgeRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def pathData: Seq[Table[DataFrame]] = Seq.empty

      override def vertexData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("Cat"),
          data = {
            // All columns of c are preserved, except for the label column.
            val columns = bindingTable.columns.filter(_.startsWith("c")) diff Seq(s"c$$$labelCol")
            bindingTable.select(columns.head, columns.tail: _*)
          }
        )
      )

      override def edgeData: Seq[Table[DataFrame]] = Seq.empty
    }
    checkGraph(actualGraph.asInstanceOf[SparkGraph], expectedGraph)
  }

  test("VertexCreate of bound variable, new properties (expr, const, inline, SET) - " +
    "CONSTRUCT (c {dw := 2 * c.weight}) SET c.constInt := 1 MATCH (c)-[e]->(f)") {
    val vertex =
      extractConstructClauses("CONSTRUCT (c {dw := 2 * c.weight}) SET c.constInt := 1 MATCH (c)")
    val actualGraph = sparkPlanner.constructGraph(bindingTable, vertex)
    val expectedGraph = new SparkGraph {
      override def graphName: String = FOO_GRAPH_NAME

      override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def edgeRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def pathData: Seq[Table[DataFrame]] = Seq.empty

      override def vertexData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("Cat"),
          data = {
            val columns = bindingTable.columns.filter(_.startsWith("c")) diff Seq(s"c$$$labelCol")
            bindingTable
              .select(columns.head, columns.tail: _*)
              .withColumn("c$constInt", lit(1))
              .withColumn("c$dw", expr("2 * `c$weight`"))
          }
        )
      )

      override def edgeData: Seq[Table[DataFrame]] = Seq.empty
    }
    checkGraph(actualGraph.asInstanceOf[SparkGraph], expectedGraph)
  }

  // TODO: Removing a label is not correctly implemented yet.
  ignore("VertexCreate of bound variable, remove property and label - " +
    "CONSTRUCT (c) REMOVE c.onDiet REMOVE c:Cat MATCH (c)") {
    val vertex = extractConstructClauses("CONSTRUCT (c) REMOVE c.onDiet REMOVE c:Cat MATCH (c)")
    val actualGraph = sparkPlanner.constructGraph(bindingTable, vertex)
    val expectedGraph = new SparkGraph {
      override def graphName: String = FOO_GRAPH_NAME

      override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def edgeRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def pathData: Seq[Table[DataFrame]] = Seq.empty

      override def vertexData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("Cat"), // TODO: Change this label to the actual one.
          data = {
            val columns =
              bindingTable.columns.filter(_.startsWith("c")) diff Seq("c$onDiet", s"c$labelCol")
            bindingTable.select(columns.head, columns.tail: _*)
          }
        )
      )

      override def edgeData: Seq[Table[DataFrame]] = Seq.empty
    }
    checkGraph(actualGraph.asInstanceOf[SparkGraph], expectedGraph)
  }

  test("VertexCreate of bound variable, filter binding table - " +
    "CONSTRUCT (c) WHEN c.age >= 5 MATCH (c)-[e]->(f)") {
    val vertex = extractConstructClauses("CONSTRUCT (c) WHEN c.age >= 5 MATCH (c)")
    val actualGraph = sparkPlanner.constructGraph(bindingTable, vertex)
    val expectedGraph = new SparkGraph {
      override def graphName: String = FOO_GRAPH_NAME

      override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def edgeRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def pathData: Seq[Table[DataFrame]] = Seq.empty

      override def vertexData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("Cat"),
          data = {
            val columns = bindingTable.columns.filter(_.startsWith("c")) diff Seq(s"c$$$labelCol")
            bindingTable.select(columns.head, columns.tail: _*).where("`c$age` >= 5")
          })
      )

      override def edgeData: Seq[Table[DataFrame]] = Seq.empty
    }
    checkGraph(actualGraph.asInstanceOf[SparkGraph], expectedGraph)
  }

  // TODO: Remove label assignment of x, once we fix the multiple/missing label(s).
  ignore("VertexCreate of unbound variable - CONSTRUCT (x) MATCH (c)") {
    val vertex = extractConstructClauses("CONSTRUCT (x) MATCH (c)")
    val actualGraph = sparkPlanner.constructGraph(bindingTable, vertex)
    val expectedGraph = new SparkGraph {
      override def graphName: String = FOO_GRAPH_NAME

      override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def edgeRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def pathData: Seq[Table[DataFrame]] = Seq.empty

      override def vertexData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("Xlabel"),
          data = bindingTable.withColumn(s"x$$$idCol", lit(1)).select(s"x$$$idCol"))
      )

      override def edgeData: Seq[Table[DataFrame]] = Seq.empty
    }
    checkGraph(actualGraph.asInstanceOf[SparkGraph], expectedGraph)
  }

  test("VertexCreate of unbound variable, add prop and label - " +
    "CONSTRUCT (x :XLabel {constInt := 1}) MATCH (c)") {
    val vertex = extractConstructClauses("CONSTRUCT (x :XLabel {constInt := 1}) MATCH (c)")
    val actualGraph = sparkPlanner.constructGraph(bindingTable, vertex)
    val expectedGraph = new SparkGraph {
      override def graphName: String = FOO_GRAPH_NAME

      override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def edgeRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def pathData: Seq[Table[DataFrame]] = Seq.empty

      override def vertexData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("XLabel"),
          data =
            bindingTable
              .withColumn(s"x$$$idCol", lit(1))
              .withColumn("x$constInt", lit(1))
              .select(s"x$$$idCol", "x$constInt"))
      )

      override def edgeData: Seq[Table[DataFrame]] = Seq.empty
    }
    checkGraph(actualGraph.asInstanceOf[SparkGraph], expectedGraph)
  }

  // TODO: Remove label assignment of x, once we fix the multiple/missing label(s).
  test("VertexCreate of unbound variable, GROUP binding table - " +
    "CONSTRUCT (x GROUP c.onDiet) MATCH (c)") {
    val vertex = extractConstructClauses("CONSTRUCT (x GROUP c.onDiet :XLabel) MATCH (c)")
    val actualGraph = sparkPlanner.constructGraph(bindingTable, vertex)
    val expectedGraph = new SparkGraph {
      override def graphName: String = FOO_GRAPH_NAME

      override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def edgeRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def pathData: Seq[Table[DataFrame]] = Seq.empty

      override def vertexData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("XLabel"),
          data =
            bindingTable
              .groupBy("c$onDiet")
              .agg(first(s"c$$$idCol"))
              .withColumn(s"x$$$idCol", lit(1))
              .select(s"x$$$idCol"))
      )

      override def edgeData: Seq[Table[DataFrame]] = Seq.empty
    }
    checkGraph(actualGraph.asInstanceOf[SparkGraph], expectedGraph)
  }

  // TODO: Remove label assignment of x, once we fix the multiple/missing label(s).
  test("VertexCreate of unbound variable, GROUP binding table, aggregate prop - " +
    "CONSTRUCT (x GROUP c.onDiet {avgw := AVG(c.weight)}) MATCH (c)") {
    val vertex =
      extractConstructClauses(
        "CONSTRUCT (x GROUP c.onDiet :XLabel {avgw := AVG(c.weight)}) MATCH (c)")
    val actualGraph = sparkPlanner.constructGraph(bindingTable, vertex)
    val expectedGraph = new SparkGraph {
      override def graphName: String = FOO_GRAPH_NAME

      override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def edgeRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def pathData: Seq[Table[DataFrame]] = Seq.empty

      override def vertexData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("XLabel"),
          data =
            bindingTable
              .groupBy("c$onDiet")
              .agg(avg("c$weight") as "x$avgw")
              .withColumn(s"x$$$idCol", lit(1))
              .select(s"x$$$idCol", "x$avgw"))
      )

      override def edgeData: Seq[Table[DataFrame]] = Seq.empty
    }
    checkGraph(actualGraph.asInstanceOf[SparkGraph], expectedGraph)
  }

  test("EdgeCreate of bound edge and endpoints - CONSTRUCT (c)-[e]->(f) MATCH (c)-[e]->(f)") {
    val edge = extractConstructClauses("CONSTRUCT (c)-[e]->(f) MATCH (c)-[e]->(f)")
    val actualGraph = sparkPlanner.constructGraph(bindingTable, edge)
    val expectedGraph = new SparkGraph {
      override def graphName: String = FOO_GRAPH_NAME

      override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def edgeRestrictions: LabelRestrictionMap = SchemaMap(
        Map(Label("Eats") -> (Label("Cat"), Label("Food")))
      )

      override def pathData: Seq[Table[DataFrame]] = Seq.empty

      override def vertexData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("Cat"),
          data = {
            val columns = bindingTable.columns.filter(_.startsWith("c")) diff Seq(s"c$$$labelCol")
            bindingTable.select(columns.head, columns.tail: _*)
          }),
        Table(
          name = Label("Food"),
          data = {
            val columns = bindingTable.columns.filter(_.startsWith("f")) diff Seq(s"f$$$labelCol")
            bindingTable.select(columns.head, columns.tail: _*)
          })
      )

      override def edgeData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("Eats"),
          data = {
            val columns = bindingTable.columns.filter(_.startsWith("e")) diff Seq(s"e$$$labelCol")
            bindingTable.select(columns.head, columns.tail: _*)
          })
      )
    }
    checkGraph(actualGraph.asInstanceOf[SparkGraph], expectedGraph, Some(EqEdgeFromTo))
  }

  // TODO: Remove b's label once the missing/multiple labels problems is fixed.
  test("EdgeCreate of bound edge, one bound and one unbound endpoint - " +
    "CONSTRUCT (c)-[e]->(b) MATCH (c)-[e]->(f)") {
    val edge = extractConstructClauses("CONSTRUCT (c)-[e]->(b :BLabel) MATCH (c)-[e]->(f)")
    val actualGraph = sparkPlanner.constructGraph(bindingTable, edge)
    val expectedBtable = bindingTable.withColumn(s"b$$$idCol", lit(1))

    val expectedGraph = new SparkGraph {
      override def graphName: String = FOO_GRAPH_NAME

      override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def edgeRestrictions: LabelRestrictionMap = SchemaMap(
        Map(Label("Eats") -> (Label("Cat"), Label("BLabel")))
      )

      override def pathData: Seq[Table[DataFrame]] = Seq.empty

      override def vertexData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("Cat"),
          data = {
            val columns = bindingTable.columns.filter(_.startsWith("c")) diff Seq(s"c$$$labelCol")
            bindingTable.select(columns.head, columns.tail: _*)
          }),
        Table(
          name = Label("BLabel"),
          data = expectedBtable.select(s"b$$$idCol"))
      )

      override def edgeData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("Eats"),
          data = {
            val columns = bindingTable.columns.filter(_.startsWith("e")) diff Seq(s"e$$$labelCol")
            expectedBtable.select(columns.head, columns.tail: _*)
          })
      )
    }
    checkGraph(actualGraph.asInstanceOf[SparkGraph], expectedGraph, Some(EqEdgeFrom))
  }

  // TODO: Remove a's and b's labels once we fix the issue with missing or multiple labels.
  test("EdgeCreate of bound edge, two unbound endpoints - " +
    "CONSTRUCT (a)-[e]->(b) MATCH (c)-[e]->(f)") {
    val edge = extractConstructClauses("CONSTRUCT (a :ALabel)-[e]->(b :BLabel) MATCH (c)-[e]->(f)")
    val actualGraph = sparkPlanner.constructGraph(bindingTable, edge)
    val expectedBtable =
      bindingTable
        .drop(s"e$$$labelCol")
        .withColumn(s"a$$$idCol", lit(1))
        .withColumn(s"b$$$idCol", lit(2))

    val expectedGraph = new SparkGraph {
      override def graphName: String = FOO_GRAPH_NAME

      override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def edgeRestrictions: LabelRestrictionMap = SchemaMap(
        Map(Label("Eats") -> (Label("ALabel"), Label("BLabel")))
      )

      override def pathData: Seq[Table[DataFrame]] = Seq.empty

      override def vertexData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("ALabel"),
          data = expectedBtable.select(s"a$$$idCol")),
        Table(
          name = Label("BLabel"),
          data = expectedBtable.select(s"b$$$idCol"))
      )

      override def edgeData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("Eats"),
          data = {
            // All columns of e are preserved, except for the label column.
            val columns = expectedBtable.columns.filter(_.startsWith("e"))
            expectedBtable.select(columns.head, columns.tail: _*)
          })
      )
    }
    checkGraph(actualGraph.asInstanceOf[SparkGraph], expectedGraph, Some(EqEdge))
  }

  // TODO: Remove labels once we fix the issue with missing or multiple labels.
  test("EdgeCreate of unbound edge, two bound endpoints - " +
    "CONSTRUCT (c)-[x]-(f) MATCH (c)-[e]->(f)") {
    val edge = extractConstructClauses("CONSTRUCT (c)-[x :XLabel]-(f) MATCH (c)-[e]->(f)")
    val actualGraph = sparkPlanner.constructGraph(bindingTable, edge)
    val expectedGraph = new SparkGraph {
      override def graphName: String = FOO_GRAPH_NAME

      override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def edgeRestrictions: LabelRestrictionMap = SchemaMap(
        Map(Label("XLabel") -> (Label("Cat"), Label("Food")))
      )

      override def pathData: Seq[Table[DataFrame]] = Seq.empty

      override def vertexData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("Cat"),
          data = {
            val columns = bindingTable.columns.filter(_.startsWith("c")) diff Seq(s"c$$$labelCol")
            bindingTable.select(columns.head, columns.tail: _*)
          }),
        Table(
          name = Label("Food"),
          data = {
            // All columns of f are preserved, except for the label column.
            val columns = bindingTable.columns.filter(_.startsWith("f")) diff Seq(s"f$$$labelCol")
            bindingTable.select(columns.head, columns.tail: _*)
          })
      )

      override def edgeData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("XLabel"),
          data =
            bindingTable
              .withColumn(s"x$$$idCol", lit(1))
              .withColumn(s"x$$$fromIdCol", expr(s"`e$$$fromIdCol`"))
              .withColumn(s"x$$$toIdCol", expr(s"`e$$$toIdCol`"))
              .select(s"x$$$idCol", s"x$$$fromIdCol", s"x$$$toIdCol"))
      )
    }
    checkGraph(actualGraph.asInstanceOf[SparkGraph], expectedGraph, Some(EqFromTo))
  }

  // TODO: Remove x, a, b labels, once we fix issue with multiple/missing labels.
  test("EdgeCreate of unbound edge, two unbound endpoints - " +
    "CONSTRUCT (a)-[x]-(b) MATCH (c)-[e]->(f)") {
    val edge =
      extractConstructClauses("CONSTRUCT (a :ALabel)-[x :XLabel]-(b :BLabel) MATCH (c)-[e]->(f)")
    val actualGraph = sparkPlanner.constructGraph(bindingTable, edge)
    val expectedBtable =
      bindingTable
        .withColumn(s"a$$$idCol", lit(1))
        .withColumn(s"b$$$idCol", lit(2))
        .withColumn(s"x$$$idCol", lit(3))
        .withColumn(s"x$$$fromIdCol", expr(s"`a$$$idCol`"))
        .withColumn(s"x$$$toIdCol", expr(s"`b$$$idCol`"))

    val expectedGraph = new SparkGraph {
      override def graphName: String = FOO_GRAPH_NAME

      override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def edgeRestrictions: LabelRestrictionMap = SchemaMap(
        Map(Label("XLabel") -> (Label("ALabel"), Label("BLabel")))
      )

      override def pathData: Seq[Table[DataFrame]] = Seq.empty

      override def vertexData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("ALabel"),
          data = expectedBtable.select(s"a$$$idCol")),
        Table(
          name = Label("BLabel"),
          data = expectedBtable.select(s"b$$$idCol"))
      )

      override def edgeData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("XLabel"),
          data = expectedBtable.select(s"x$$$idCol", s"x$$$fromIdCol", s"x$$$toIdCol"))
      )
    }
    checkGraph(actualGraph.asInstanceOf[SparkGraph], expectedGraph, Some(EqEdge))
  }

  // TODO: Unignore once we fix issue with multiple/missing labels.
  ignore("EdgeCreate of unbound edge, one bound endpoint, one unbound grouped endpoint - " +
    "CONSTRUCT (c)-[x]->(d GROUP c.onDiet) MATCH (c)-[e]->(f)") {
    val edge =
      extractConstructClauses(
        "CONSTRUCT (c)-[x]->(d GROUP c.onDiet) MATCH (c)-[e]->(f)")
    val actualGraph = sparkPlanner.constructGraph(bindingTable, edge)
    val expectedGraph = new SparkGraph {
      override def graphName: String = FOO_GRAPH_NAME

      override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def edgeRestrictions: LabelRestrictionMap = SchemaMap(
        Map(Label("XLabel") -> (Label("Cat"), Label("DLabel")))
      )

      override def pathData: Seq[Table[DataFrame]] = Seq.empty

      override def vertexData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("Cat"),
          data = {
            // All columns of c are preserved, except for the label column.
            val columns = bindingTable.columns.filter(_.startsWith("c")) diff Seq(s"c$$$labelCol")
            bindingTable.select(columns.head, columns.tail: _*)
          }),
        Table(
          name = Label("DLabel"),
          data =
            bindingTable
              .groupBy("c$onDiet")
              .agg(first(s"c$$$idCol"))
              .withColumn(s"d$$$idCol", lit(1))
              .select(s"d$$$idCol"))
      )

      override def edgeData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("XLabel"),
          data =
            bindingTable
              .withColumn(s"x$$$idCol", lit(1))
              .withColumn(s"x$$$fromIdCol", expr(s"`e$$$fromIdCol`"))
              .withColumn(s"x$$$toIdCol", expr(s"`e$$$toIdCol`"))
              .select(s"x$$$idCol", s"x$$$fromIdCol", s"x$$$toIdCol"))
      )
    }
    checkGraph(actualGraph.asInstanceOf[SparkGraph], expectedGraph, Some(EqEdge))
  }

  test("EdgeCreate with new properties and labels for endpoints and connection - " +
    "CONSTRUCT (c)-[x :OnDiet]->(d GROUP c.onDiet :Boolean {val := c.onDiet}) " +
    "MATCH (c)-[e]->(f)") {
    val edge =
      extractConstructClauses(
        "CONSTRUCT (c)-[x :OnDiet]->(d GROUP c.onDiet :Boolean {val := c.onDiet}) " +
          "MATCH (c)-[e]->(f)")
    val actualGraph = sparkPlanner.constructGraph(bindingTable, edge)
    val expectedGraph = new SparkGraph {
      override def graphName: String = FOO_GRAPH_NAME

      override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def edgeRestrictions: LabelRestrictionMap = SchemaMap(
        Map(Label("OnDiet") -> (Label("Cat"), Label("Boolean")))
      )

      override def pathData: Seq[Table[DataFrame]] = Seq.empty

      override def vertexData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("Cat"),
          data = {
            val columns = bindingTable.columns.filter(_.startsWith("c")) diff Seq(s"c$$$labelCol")
            bindingTable.select(columns.head, columns.tail: _*)
          }),
        Table(
          name = Label("Boolean"),
          data =
            bindingTable
              .groupBy("c$onDiet")
              .agg(first(s"c$$$idCol"))
              .withColumn(s"d$$$idCol", lit(1))
              .withColumn("d$val", expr("`c$onDiet`"))
              .select(s"d$$$idCol", "d$val"))
      )

      override def edgeData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("OnDiet"),
          data =
            bindingTable
              .withColumn(s"x$$$idCol", lit(1))
              .withColumn(s"x$$$fromIdCol", expr(s"`e$$$fromIdCol`"))
              .withColumn(s"x$$$toIdCol", expr(s"`e$$$toIdCol`"))
              .select(s"x$$$idCol", s"x$$$fromIdCol", s"x$$$toIdCol"))
      )
    }
    checkGraph(actualGraph.asInstanceOf[SparkGraph], expectedGraph, Some(EqEdge))
  }

  // TODO: Remove label of e0, c1, f1, once we fix the issue with the labels.
  test("EdgeCreate from duplicate pairs of endpoints, check implicit grouping is used - " +
    "CONSTRUCT (c1)-[e0]->(f1) MATCH (c1)-[e1]->(f1), (c2)-[e2]->(f2) (cross-join of patterns)") {
    val edge =
      extractConstructClauses(
        "CONSTRUCT (c1 :CLabel)-[e0 :ELabel]->(f1 :FLabel) MATCH (c1)-[e1]->(f1), (c2)-[e2]->(f2)")
    val actualGraph = sparkPlanner.constructGraph(bindingTableDuplicateData, edge)

    // The number of new edges e0 must be equal to the number of unique pairs (c1, f1).
    val e0ids =
      actualGraph.asInstanceOf[SparkGraph]
        .edgeData.head.data
        .select(s"$idCol").collect().map(_(0))
    val vertexGroups =
      bindingTableDuplicateData
        .select(s"c1$$$idCol", s"f1$$$idCol")
        .collect()
        .map(row => (row(0).toString, row(1).toString))
        .toSet
    assert(e0ids.toSet.size == vertexGroups.size)
  }

  // TODO: Remove label of e0, e1 once we fix the issue with the labels.
  test("GroupConstruct of bound endpoints, two unbound edges - " +
    "CONSTRUCT (c)-[e0]->(f)-[e1]->(c) MATCH (c)-[e]->(f)") {
    val group =
      extractConstructClauses(
        "CONSTRUCT (c)-[e0 :e0Label]->(f)-[e1 :e1Label]->(c) MATCH (c)-[e]->(f)")
    val actualGraph = sparkPlanner.constructGraph(bindingTable, group)
    val expectedGraph = new SparkGraph {
      override def graphName: String = FOO_GRAPH_NAME

      override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def edgeRestrictions: LabelRestrictionMap = SchemaMap(
        Map(
          Label("e0Label") -> (Label("Cat"), Label("Food")),
          Label("e1Label") -> (Label("Food"), Label("Cat")))
      )

      override def pathData: Seq[Table[DataFrame]] = Seq.empty

      override def vertexData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("Cat"),
          data = {
            val columns = bindingTable.columns.filter(_.startsWith("c")) diff Seq(s"c$$$labelCol")
            bindingTable.select(columns.head, columns.tail: _*)
          }),
        Table(
          name = Label("Food"),
          data = {
            val columns = bindingTable.columns.filter(_.startsWith("f")) diff Seq(s"f$$$labelCol")
            bindingTable.select(columns.head, columns.tail: _*)
          })
      )

      override def edgeData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("e0Label"),
          data =
            bindingTable
              .withColumn(s"e0$$$idCol", lit(1))
              .withColumn(s"e0$$$fromIdCol", expr(s"`e$$$fromIdCol`"))
              .withColumn(s"e0$$$toIdCol", expr(s"`e$$$toIdCol`"))
              .select(s"e0$$$idCol", s"e0$$$fromIdCol", s"e0$$$toIdCol")),
        Table(
          name = Label("e1Label"),
          data =
            bindingTable
              .withColumn(s"e1$$$idCol", lit(1))
              .withColumn(s"e1$$$fromIdCol", expr(s"`e$$$toIdCol`"))
              .withColumn(s"e1$$$toIdCol", expr(s"`e$$$fromIdCol`"))
              .select(s"e1$$$idCol", s"e1$$$fromIdCol", s"e1$$$toIdCol"))
      )
    }
    checkGraph(actualGraph.asInstanceOf[SparkGraph], expectedGraph, Some(EqFromTo))
  }

  // TODO: Remove label of e0, e1, x once we fix the issue with the labels.
  test("GroupConstruct, add one vertex between matched endpoints - " +
    "CONSTRUCT (c)-[e0]->(x)-[e1]->(f) MATCH (c)-[e]->(f)") {
    val group = extractConstructClauses(
      "CONSTRUCT (c)-[e0 :e0Label]->(x :XLabel)-[e1 :e1Label]->(f) MATCH (c)-[e]->(f)")
    val actualGraph = sparkPlanner.constructGraph(bindingTable, group)
    val expectedGraph = new SparkGraph {
      override def graphName: String = FOO_GRAPH_NAME

      override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def edgeRestrictions: LabelRestrictionMap = SchemaMap(
        Map(
          Label("e0Label") -> (Label("Cat"), Label("XLabel")),
          Label("e1Label") -> (Label("XLabel"), Label("Food")))
      )

      override def pathData: Seq[Table[DataFrame]] = Seq.empty

      override def vertexData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("Cat"),
          data = {
            val columns = bindingTable.columns.filter(_.startsWith("c")) diff Seq(s"c$$$labelCol")
            bindingTable.select(columns.head, columns.tail: _*)
          }),
        Table(
          name = Label("Food"),
          data = {
            val columns = bindingTable.columns.filter(_.startsWith("f")) diff Seq(s"f$$$labelCol")
            bindingTable.select(columns.head, columns.tail: _*)
          }),
        Table(
          name = Label("XLabel"),
          data = bindingTable.withColumn(s"x$$$idCol", lit(1)).select(s"x$$$idCol"))
      )

      override def edgeData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("e0Label"),
          data =
            bindingTable
              .withColumn(s"e0$$$idCol", lit(1))
              .withColumn(s"e0$$$fromIdCol", expr(s"`e$$$fromIdCol`"))
              .withColumn(s"e0$$$toIdCol", expr(s"`e$$$toIdCol`"))
              .select(s"e0$$$idCol", s"e0$$$fromIdCol", s"e0$$$toIdCol")),
        Table(
          name = Label("e1Label"),
          data =
            bindingTable
              .withColumn(s"e1$$$idCol", lit(1))
              .withColumn(s"e1$$$fromIdCol", expr(s"`e$$$toIdCol`"))
              .withColumn(s"e1$$$toIdCol", expr(s"`e$$$fromIdCol`"))
              .select(s"e1$$$idCol", s"e1$$$fromIdCol", s"e1$$$toIdCol"))
      )
    }
    checkGraph(actualGraph.asInstanceOf[SparkGraph], expectedGraph, Some(EqEdge))
  }

  // TODO: Remove label of e0, e1, d once we fix the issue with the labels.
  test("GroupConstruct with GROUP-ing - " +
    "CONSTRUCT (d GROUP c.onDiet)<-(c)->(f) MATCH (c)-[e]->(f)") {
    val group =
      extractConstructClauses(
        "CONSTRUCT (d GROUP c.onDiet :DLabel)<-[e0 :e0Label]-(c)-[e1 :e1Label]->(f) " +
          "MATCH (c)-[e]->(f)")
    val actualGraph = sparkPlanner.constructGraph(bindingTable, group)
    val expectedGraph = new SparkGraph {
      override def graphName: String = FOO_GRAPH_NAME

      override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def edgeRestrictions: LabelRestrictionMap = SchemaMap(
        Map(
          Label("e0Label") -> (Label("Cat"), Label("DLabel")),
          Label("e1Label") -> (Label("Cat"), Label("Food")))
      )

      override def pathData: Seq[Table[DataFrame]] = Seq.empty

      override def vertexData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("Cat"),
          data = {
            val columns = bindingTable.columns.filter(_.startsWith("c")) diff Seq(s"c$$$labelCol")
            bindingTable.select(columns.head, columns.tail: _*)
          }),
        Table(
          name = Label("Food"),
          data = {
            val columns = bindingTable.columns.filter(_.startsWith("f")) diff Seq(s"f$$$labelCol")
            bindingTable.select(columns.head, columns.tail: _*)
          }),
        Table(
          name = Label("DLabel"),
          data =
            bindingTable
              .groupBy("c$onDiet")
              .agg(first(s"c$$$idCol"))
              .withColumn(s"d$$$idCol", lit(1))
              .select(s"d$$$idCol"))
      )

      override def edgeData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("e0Label"),
          data =
            bindingTable
              .withColumn(s"e0$$$idCol", lit(1))
              .withColumn(s"e0$$$fromIdCol", expr(s"`e$$$toIdCol`"))
              .withColumn(s"e0$$$toIdCol", expr(s"`e$$$fromIdCol`"))
              .select(s"e0$$$idCol", s"e0$$$fromIdCol", s"e0$$$toIdCol")),
        Table(
          name = Label("e1Label"),
          data =
            bindingTable
              .withColumn(s"e1$$$idCol", lit(1))
              .withColumn(s"e1$$$fromIdCol", expr(s"`e$$$fromIdCol`"))
              .withColumn(s"e1$$$toIdCol", expr(s"`e$$$toIdCol`"))
              .select(s"e1$$$idCol", s"e1$$$fromIdCol", s"e1$$$toIdCol"))
      )
    }
    checkGraph(actualGraph.asInstanceOf[SparkGraph], expectedGraph, Some(EqEdge))
  }

  test("GroupConstruct of the same vertex with contradicting filtering yields the empty table - " +
    "CONSTRUCT (c) WHEN c.age <= 3, (c) WHEN c.age > 3 MATCH (c)") {
    val group =
      extractConstructClauses(
        query = "CONSTRUCT (c) WHEN c.age <= 3, (c) WHEN c.age > 3 MATCH (c)",
        expectedNumClauses = 1) // one clause, both BasicConstructs are on the same vertex
    val actualGraph = sparkPlanner.constructGraph(bindingTable, group)
    assert(actualGraph.isEmpty)
  }

  test("GroupConstruct of two vertices, with filtering - " +
    "CONSTRUCT (c) WHEN c.age <= 3, (f) WHEN c.age <= 3 MATCH (c)-[e]->(f)") {
    val group =
      extractConstructClauses(
        query = "CONSTRUCT (c) WHEN c.age <= 3, (f) WHEN c.age <= 3 MATCH (c)-[e]->(f)",
        expectedNumClauses = 2)
    val actualGraph = sparkPlanner.constructGraph(bindingTable, group)
    val expectedGraph = new SparkGraph {
      override def graphName: String = FOO_GRAPH_NAME

      override def storedPathRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def edgeRestrictions: LabelRestrictionMap = SchemaMap.empty

      override def pathData: Seq[Table[DataFrame]] = Seq.empty

      override def vertexData: Seq[Table[DataFrame]] = Seq(
        Table(
          name = Label("Cat"),
          data = {
            val columns = bindingTable.columns.filter(_.startsWith("c")) diff Seq(s"c$$$labelCol")
            bindingTable.select(columns.head, columns.tail: _*).where("`c$age` <= 3")
          }),
        Table(
          name = Label("Food"),
          data = {
            val columns = bindingTable.columns.filter(_.startsWith("f")) diff Seq(s"f$$$labelCol")
            bindingTable.select(columns.head, columns.tail: _*).where("`c$age` <= 3")
          })
      )

      override def edgeData: Seq[Table[DataFrame]] = Seq.empty
    }
    checkGraph(actualGraph.asInstanceOf[SparkGraph], expectedGraph)
  }

  private def extractConstructClauses(query: String, expectedNumClauses: Int = 1)
  : Seq[AlgebraTreeNode] = {

    val createGraph = (parser andThen algebraRewriter) (query)
    val constructClauses = createGraph.asInstanceOf[GraphCreate].constructClauses

    assert(constructClauses.size == expectedNumClauses)

    constructClauses
  }

  private def checkGraph(actualGraph: SparkGraph,
                         expectedGraph: SparkGraph,
                         edgeEqualFn: Option[(Table[DataFrame], Table[DataFrame]) => EqEdgeBase] = None)
  : Unit = {

    // Check that the edge and path restrictions are the expected ones.
    assert(actualGraph.edgeRestrictions == expectedGraph.edgeRestrictions)
    assert(actualGraph.storedPathRestrictions == expectedGraph.storedPathRestrictions)

    // For each entity type, check that we have the expected tables.
    checkTables(actualGraph.vertexData, expectedGraph.vertexData, EqVertex)

    if (edgeEqualFn.isDefined)
      checkTables(actualGraph.edgeData, expectedGraph.edgeData, edgeEqualFn.get)
  }

  private def checkTables(actualTables: Seq[Table[DataFrame]],
                          expectedTables: Seq[Table[DataFrame]],
                          equalFn: (Table[DataFrame], Table[DataFrame]) => EqBase): Unit = {
    // Check we have the same number of tables as expected for this entity.
    assert(actualTables.size == expectedTables.size)

    val actualTableMap: Map[Label, Table[DataFrame]] = {
      val labels: Seq[Label] = actualTables.map(table => table.name)
      assert(labels.size == labels.distinct.size) // no duplicate labels
      actualTables.map(table => table.name -> table).toMap
    }

    val expectedTableMap: Map[Label, Table[DataFrame]] =
      expectedTables.map(table => table.name -> table).toMap

    // Check we have the same labels as expected for this entity.
    assert(actualTableMap.keySet == expectedTableMap.keySet)

    // For each label, check that data correspond to the expected data.
    actualTableMap.foreach {
      case (label, actualTable) => equalFn(actualTable, expectedTableMap(label)).assertEqual()
    }
  }

  /**
    * Base equals-like operator to test that two [[DataFrame]] [[Table]]s are equal. The operator
    * checks that the actual and expected [[Table.name]]s are equal, that the actual and expected
    * headers are equal, that there is a correct number of unique ids as an id interval and that
    * the data contained by the two [[Table]]s is equal. Note that the [[ID_COL]] does not
    * participate in the data equality test.
    */
  sealed abstract class EqBase(actualTable: Table[DataFrame],
                               expectedTable: Table[DataFrame],
                               idColumnNames: Seq[String]) {

    val expectedTableColumnsRenamed: DataFrame = {
      val expectedColumnsRenamed: Seq[String] =
        expectedTable.data.columns.map(column => column.split("\\$")(1))
      expectedTable.data.toDF(expectedColumnsRenamed: _*)
    }

    val actualHeader: Seq[String] = actualTable.data.columns

    val collectedActualIds: Seq[Long] =
      actualTable.data.select(idCol).collect().map(_.get(0).toString.toLong)

    def assertEqual(): Unit = {
      assert(actualTable.name == expectedTable.name)
      compareHeaders(actualHeader, expectedTableColumnsRenamed)
      assertCorrectIds()
      assertEqualDataWithoutId()
    }

    private def assertCorrectIds(): Unit = {
      val actualIds: Seq[Long] = collectedActualIds
      val baseId: Long =
        START_BASE_TABLE_INDEX +
          (actualIds.head - START_BASE_TABLE_INDEX) / TABLE_INDEX_INCREMENT * TABLE_INDEX_INCREMENT
      val expectedIds: Seq[Long] = baseId until (baseId + actualIds.size)
      assert(actualIds.size == actualIds.distinct.size)
      assert(actualIds.toSet == expectedIds.toSet)
    }

    private def assertEqualDataWithoutId(): Unit = {
      val headerWithoutId: Seq[String] = actualHeader diff idColumnNames
      if (headerWithoutId.nonEmpty)
        compareDfs(
          actualTable.data.select(headerWithoutId.head, headerWithoutId.tail: _*),
          expectedTableColumnsRenamed.select(headerWithoutId.head, headerWithoutId.tail: _*))
    }
  }

  /** The equals-like operator to test that two vertex [[Table]]s are equal. */
  sealed case class EqVertex(actualTable: Table[DataFrame], expectedTable: Table[DataFrame])
    extends EqBase(actualTable, expectedTable, Seq(idCol))

  /** The base equals-like operator to test the equality of two edge [[Table]]s. */
  sealed abstract class EqEdgeBase(actualTable: Table[DataFrame], expectedTable: Table[DataFrame])
    extends EqBase(actualTable, expectedTable, Seq(idCol, fromIdCol, toIdCol)) {

    val btableIdTuples: Seq[(Long, Long, Long)] =
      expectedTableColumnsRenamed
        .select(idCol, fromIdCol, toIdCol)
        .collect()
        .map(row =>
          (row.get(0).toString.toLong, row.get(1).toString.toLong, row.get(2).toString.toLong))
    val actualIdTuples: Seq[(Long, Long, Long)] =
      actualTable.data
        .select(idCol, fromIdCol, toIdCol)
        .collect()
        .map(row =>
          (row.get(0).toString.toLong, row.get(1).toString.toLong, row.get(2).toString.toLong))
    val btableEdgeIds: Seq[Long] = btableIdTuples.map(tuple => tuple._1).sorted
    val actualEdgeIds: Seq[Long] = actualIdTuples.map(tuple => tuple._1).sorted
    val btableFromIds: Seq[Long] = btableIdTuples.map(tuple => tuple._2).sorted
    val actualFromIds: Seq[Long] = actualIdTuples.map(tuple => tuple._2).sorted
    val btableToIds: Seq[Long] = btableIdTuples.map(tuple => tuple._3).sorted
    val actualToIds: Seq[Long] = actualIdTuples.map(tuple => tuple._3).sorted

    val btableTupleMap: Map[Long, (Long, Long)] =
      btableIdTuples.map(tuple => tuple._1 -> (tuple._2, tuple._3)).toMap

    val edgeActualToBtableId: Map[Long, Long] = (actualEdgeIds zip btableEdgeIds).toMap

    val fromBtableToActualId: Map[Long, Long] = (btableFromIds zip actualFromIds).toMap

    val toBtableToActualId: Map[Long, Long] = (btableToIds zip actualToIds).toMap

    override val collectedActualIds: Seq[Long] = actualEdgeIds
  }

  sealed case class EqEdge(actualTable: Table[DataFrame], expectedTable: Table[DataFrame])
    extends EqEdgeBase(actualTable, expectedTable)

  /**
    * An [[EqEdgeBase]] operator that also checks whether the correlation between edge, source and
    * destination ids is the same in the actual and expected tables. Should be used when testing
    * CONSTRUCT clauses in which both endpoints and the edge variables have been matched.
    *
    * The expected table is built from the binding table, therefore the edge and source and
    * destination endpoint ids are the same as in the binding table. The endpoint ids are strictly
    * determined by the edge id - each unique edge id determines a unique pair of endpoint ids. The
    * same applies to the actual table.
    *
    * Furthermore, we know, from the creation process of an entity, that the new ids of each entity
    * are assigned in the order of the original binding table ids (see [[EntityConstruct]]). This
    * means we can create a mapping between the binding table ids and the actual ids, from the
    * ordering of the two.
    *
    * Using the two mapping, edge to endpoints and binding table edge to actual edge, we can test
    * whether, for a given actual edge id, the actual ids of the endpoints indeed map to the
    * binding table id of that edge.
    *
    * For example, we infer the following mapping from the binding and actual table:
    * edge_id_btable -> (source_id_btable, dest_id_btable) =
    *   200 -> (100, 101),
    *   201 -> (102, 103),
    *   202 -> (104, 105)
    * edge_id_actual -> (source_id_actual, dest_id_actual) =
    *   2000 -> (1000, 1001),
    *   2001 -> (1002, 1003),
    *   2002 -> (1004, 1005)
    *
    * Then:
    * edge_id_btable = {200, 201, 202}
    * edge_id_actual = {2000, 2001, 2002}
    * Which means: edge_id_actual -> edge_id_btable =
    *   2000 -> 200
    *   2001 -> 201
    *   2002 -> 202
    *
    *
    * source_id_btable = {100, 102, 104}
    * source_id_actual = {1000, 1002, 1004}
    * Which means: source_id_btable -> source_id_actual =
    *   100 -> 1000
    *   102 -> 1002
    *   104 -> 1004
    *
    * dest_id_btable = {101, 103, 105}
    * dest_id_actual = {1001, 1003, 1005}
    * Which means: dest_id_btable -> dest_id_actual =
    *   101 -> 1001
    *   103 -> 1003
    *   105 -> 1005
    *
    * Then, for each tuple (edge_id_actual, source_id_actual, dest_id_actual):
    * - we extract the edge_id_btable from the edge_id_actual -> edge_id_btable mapping;
    * - we extract source_id_btable and dest_id_btable from the
    * edge_id_btable -> (source_id_btable, dest_id_btable) mapping;
    * - we extract expected_source_id and expected_dest_id from the
    * source_id_btable -> source_id_actual and dest_id_btable -> dest_id_actual, respectively;
    * - we assert that expected_source_id == source_id_actual and
    * expected_dest_id == dest_id_actual.
    */
  sealed case class EqEdgeFromTo(actualTable: Table[DataFrame], expectedTable: Table[DataFrame])
    extends EqEdgeBase(actualTable, expectedTable) {

    override def assertEqual(): Unit = {
      super.assertEqual()

      actualIdTuples.foreach {
        case (actualEdgeId, actualFromId, actualToId) =>
          val btableEdgeId: Long = edgeActualToBtableId(actualEdgeId)
          val btableFromToIdTuple: (Long, Long) = btableTupleMap(btableEdgeId)
          assert(fromBtableToActualId(btableFromToIdTuple._1) == actualFromId)
          assert(toBtableToActualId(btableFromToIdTuple._2) == actualToId)
      }
    }
  }

  /**
    * Same as [[EqEdgeFromTo]], though we only check the correlation between the edge and the source
    * ids. Should be used when testing CONSTRUCT clauses in which the source endpoint and the edge
    * variables have been matched.
    */
  sealed case class EqEdgeFrom(actualTable: Table[DataFrame], expectedTable: Table[DataFrame])
    extends EqEdgeBase(actualTable, expectedTable) {

    override def assertEqual(): Unit = {
      super.assertEqual()

      actualIdTuples.foreach {
        case (actualEdgeId, actualFromId, _) =>
          val btableEdgeId: Long = edgeActualToBtableId(actualEdgeId)
          val btableFromToIdTuple: (Long, Long) = btableTupleMap(btableEdgeId)
          assert(fromBtableToActualId(btableFromToIdTuple._1) == actualFromId)
      }
    }
  }

  /**
    * Same as [[EqEdgeFromTo]], though we only check the correlation between the source and the
    * destination ids. Should be used when testing CONSTRUCT clauses in which the source and
    * destinations variables have been matched.
    */
  sealed case class EqFromTo(actualTable: Table[DataFrame], expectedTable: Table[DataFrame])
    extends EqEdgeBase(actualTable, expectedTable) {

    override def assertEqual(): Unit = {
      super.assertEqual()

      val btableFromToMap: Map[Long, Seq[Long]] =
        btableIdTuples
          .map(idTuple => idTuple._2 -> idTuple._3) // from -> to
          .groupBy(_._1) // group by from
          .mapValues(tuples => tuples.map(_._2)) // extract sequence of to's
      val actualFromToMap: Map[Long, Seq[Long]] =
        actualIdTuples
          .map(idTuple => idTuple._2 -> idTuple._3) // from -> to
          .groupBy(_._1) // group by from
          .mapValues(tuples => tuples.map(_._2)) // extract sequence of to's
      val fromActualToBtableId: Map[Long, Long] = fromBtableToActualId.map(_.swap)
      val toActualToBtableId: Map[Long, Long] = toBtableToActualId.map(_.swap)

      actualFromToMap.foreach {
        case (actualFromId, actualToIdList) =>
          val btableFromId: Long = fromActualToBtableId(actualFromId)
          val btableToIds: Seq[Long] = actualToIdList.map(toActualToBtableId)
          assert(btableFromToMap(btableFromId).sorted == btableToIds.sorted)
      }
    }
  }

  /************************************** MATCH ***************************************************/
  test("Binding table of VertexScan - (c:Cat)") {
    val scan = extractMatchClause("CONSTRUCT (c) MATCH (c:Cat)")
    val actualDf = sparkPlanner.solveBindingTable(scan)

    val expectedHeader: Seq[String] =
      Seq(s"c$$$labelCol", s"c$$$idCol", "c$name", "c$age", "c$weight", "c$onDiet")
    compareHeaders(expectedHeader, actualDf)

    val expectedDf =
      Seq(coby, hosico, maru, grumpy).toDF.withColumn(TABLE_LABEL_COL.columnName, lit("Cat"))
    compareDfs(
      actualDf.select(s"c$$$labelCol", s"c$$$idCol", "c$name", "c$age", "c$weight", "c$onDiet"),
      expectedDf.select(labelCol, idCol, "name", "age", "weight", "onDiet"))
  }

  test("Binding table of EdgeScan - (c:Cat)-[e:Eats]->(f:Food)") {
    val scan = extractMatchClause("CONSTRUCT (c) MATCH (c:Cat)-[e:Eats]->(f:Food)")
    val actualDf = sparkPlanner.solveBindingTable(scan)

    val expectedHeader: Seq[String] =
      Seq(
        s"c$$$labelCol", s"c$$$idCol", "c$name", "c$age", "c$weight", "c$onDiet",
        s"e$$$labelCol", s"e$$$idCol", s"e$$$fromIdCol", s"e$$$toIdCol", "e$gramsPerDay",
        s"f$$$labelCol", s"f$$$idCol", "f$brand")
    compareHeaders(expectedHeader, actualDf)

    val expectedDf =
      createCatEatsFoodTable(
        Seq(
          (coby, cobyEatsPurina, purina), (hosico, hosicoEatsGourmand, gourmand),
          (maru, maruEatsWhiskas, whiskas), (grumpy, grumpyEatsGourmand, gourmand)),
        fromRef = "c", edgeRef = "e", toRef = "f")

    compareDfs(
      actualDf.select(expectedHeader.head, expectedHeader.tail: _*),
      expectedDf.select(
        s"c$$$labelCol", "catId", "name", "age", "weight", "onDiet",
        s"e$$$labelCol", "eatsId", s"$fromIdCol", s"$toIdCol", "gramsPerDay",
        s"f$$$labelCol", "foodId", "brand"))
  }

  test("Binding table of PathScan, isReachableTest = true - (c:Cat)-/@ /->(f:Food)") {
    val scan = extractMatchClause("CONSTRUCT (c) MATCH (c:Cat)-/@ /->(f:Food)")
    val actualDf = sparkPlanner.solveBindingTable(scan)

    /** isReachableTest = true => p's attributes are not included in the result. */
    val expectedHeader: Seq[String] =
      Seq(
        s"c$$$labelCol", s"c$$$idCol", "c$name", "c$age", "c$weight", "c$onDiet",
        s"f$$$labelCol", s"f$$$idCol", "f$brand")
    compareHeaders(expectedHeader, actualDf)

    val expectedDf =
      createCatFoodTable(
        Seq((coby, gourmand), (hosico, gourmand), (maru, gourmand), (grumpy, gourmand)),
        rightRef = "c", leftRef = "f")

    compareDfs(
      actualDf.select(expectedHeader.head, expectedHeader.tail: _*),
      expectedDf.select(
        s"c$$$labelCol", "catId", "name", "age", "weight", "onDiet",
        s"f$$$labelCol", "foodId", "brand"))
  }

  test("Binding table of PathScan, isReachableTest = false, costVarDef = cost - " +
    "(c:Cat)-/@p COST cost/->(f:Food)") {
    val scan = extractMatchClause("CONSTRUCT (c) MATCH (c:Cat)-/@p COST cost/->(f:Food)")
    val actualDf = sparkPlanner.solveBindingTable(scan)

    /**
      * isReachableTest = false => p's attributes are included in the result
      * costVarDef is defined => column "cost" is included in the result and is equal to path length
      * (path length = number of edges in the [[EDGE_SEQ_COL]] of the path.
      */
    val expectedHeader: Seq[String] =
      Seq(
        s"c$$$labelCol", s"c$$$idCol", "c$name", "c$age", "c$weight", "c$onDiet",
        s"p$$$labelCol", s"p$$$idCol", s"p$$$fromIdCol", s"p$$$toIdCol", s"p$$$edgesCol",
        "p$hops", "p$cost",
        s"f$$$labelCol", s"f$$$idCol", "f$brand")
    compareHeaders(expectedHeader, actualDf)

    val expectedDf =
      createCatToGourmandFoodTable(
        Seq(
          (coby, fromCoby, gourmand, fromCoby.hops),
          (hosico, fromHosico, gourmand, fromHosico.hops),
          (maru, fromMaru, gourmand, fromMaru.hops),
          (grumpy, fromGrumpy, gourmand, fromGrumpy.hops)),
        fromRef = "c", pathRef = "p", toRef = "f")

    compareDfs(
      actualDf.select(expectedHeader.head, expectedHeader.tail: _*),
      expectedDf.select(
        s"c$$$labelCol", "catId", "name", "age", "weight", "onDiet",
        s"p$$$labelCol", "toGourmandId", s"$fromIdCol", s"$toIdCol", s"$edgesCol", "hops", "cost",
        s"f$$$labelCol", "foodId", "brand"))
  }

  test("Binding table of UnionAll - (c1:Cat)->(c2:Cat). Each side of the union is padded with " +
    "missing columns, set to null.") {
    val union = extractMatchClause("CONSTRUCT (c) MATCH (c1:Cat)-[e]->(c2:Cat)")
    val actualDf = sparkPlanner.solveBindingTable(union)

    /**
      * [[TABLE_LABEL_COL]], [[ID_COL]], [[FROM_ID_COL]], [[TO_ID_COL]] and "since" are common
      * between the two sides. Column "fights" is only present in the right hand-side of the union.
      */
    val expectedHeader: Seq[String] =
      Seq(
        s"c1$$$labelCol", s"c1$$$idCol", "c1$name", "c1$age", "c1$weight", "c1$onDiet",
        s"e$$$labelCol", s"e$$$idCol", s"e$$$fromIdCol", s"e$$$toIdCol", "e$since", "e$fights",
        s"c2$$$labelCol", s"c2$$$idCol", "c2$name", "c2$age", "c2$weight", "c2$onDiet")
    compareHeaders(expectedHeader, actualDf)

    val expectedDf =
      createEnemyUnionFriendTable(
        lhsTuples =
          Seq((coby, cobyFriendWithHosico, hosico), (hosico, hosicoFriendWithCobby, coby)),
        rhsTuples = Seq((maru, maruEnemyGrumpy, grumpy), (grumpy, grumpyEnemyMaru, maru)),
        fromRef = "c1", toRef = "c2")

    compareDfs(
      actualDf.select(expectedHeader.head, expectedHeader.tail: _*),
      expectedDf.select(
        s"c1$$$labelCol", "c1Id", "c1Name", "c1Age", "c1Weight", "c1OnDiet",
        "label", "eId", s"$fromIdCol", s"$toIdCol", "since", "fights",
        s"c2$$$labelCol", "c2Id", "c2Name", "c2Age", "c2Weight", "c2OnDiet"))
  }

  test("Binding table of InnerJoin - (f:Food)->(c:Country), (f). Common columns used in the join " +
    "are correctly identified.") {
    val join = extractMatchClause("CONSTRUCT (c) MATCH (f:Food)-[e]->(c:Country), (f)")
    val actualDf = sparkPlanner.solveBindingTable(join)

    val expectedHeader: Seq[String] =
      Seq(
        s"f$$$labelCol", s"f$$$idCol", "f$brand",
        s"e$$$labelCol", s"e$$$idCol", s"e$$$fromIdCol", s"e$$$toIdCol",
        s"c$$$labelCol", s"c$$$idCol", "c$name")
    compareHeaders(expectedHeader, actualDf)

    val expectedDf =
      createFoodMadeInCountryTable(
        Seq(
          (purina, purinaMadeInGermany, germany),
          (whiskas, whiskasMadeInGermany, germany),
          (gourmand, gourmandMadeInFrance, france)),
        fromRef = "f", edgeRef = "e", toRef = "c")

    compareDfs(
      actualDf.select(expectedHeader.head, expectedHeader.tail: _*),
      expectedDf.select(
        s"f$$$labelCol", "foodId", "brand",
        s"e$$$labelCol", "madeInId", s"$fromIdCol", s"$toIdCol",
        s"c$$$labelCol", "countryId", "name"))
  }

  test("Binding table of CrossJoin - (f:Food), (c:Country)") {
    val join = extractMatchClause("CONSTRUCT (c) MATCH (f:Food), (c:Country)")
    val actualDf = sparkPlanner.solveBindingTable(join)

    val expectedHeader: Seq[String] =
      Seq(s"f$$$labelCol", s"f$$$idCol", "f$brand", s"c$$$labelCol", s"c$$$idCol", "c$name")
    compareHeaders(expectedHeader, actualDf)

    val expectedDf =
      createFoodCountryTable(
        Seq(
          (purina, germany), (purina, france),
          (whiskas, germany), (whiskas, france),
          (gourmand, germany), (gourmand, france)),
        lhsRef = "f", rhsRef = "c")

    compareDfs(
      actualDf.select(expectedHeader.head, expectedHeader.tail: _*),
      expectedDf.select(s"f$$$labelCol", "foodId", "brand", s"c$$$labelCol", "countryId", "name"))
  }

  test("Binding table of LeftOuterJoin - (c1:Cat) OPTIONAL (c1)-[:Enemy]->(c2). Common columns " +
    "used in the join are correctly identified.") {
    val join = extractMatchClause("CONSTRUCT (c) MATCH (c1:Cat) OPTIONAL (c1)-[e:Enemy]->(c2)")
    val actualDf = sparkPlanner.solveBindingTable(join)

    val expectedHeader: Seq[String] =
      Seq(
        s"c1$$$labelCol", s"c1$$$idCol", "c1$name", "c1$age", "c1$weight", "c1$onDiet",
        s"e$$$labelCol", s"e$$$idCol", s"e$$$fromIdCol", s"e$$$toIdCol", "e$since", "e$fights",
        s"c2$$$labelCol", s"c2$$$idCol", "c2$name", "c2$age", "c2$weight", "c2$onDiet")
    compareHeaders(expectedHeader, actualDf)

    val expectedDf =
      createCatEnemyCatTable(
        nullableTuples = Seq(coby, hosico),
        matchedTuples = Seq((maru, maruEnemyGrumpy, grumpy), (grumpy, grumpyEnemyMaru, maru)),
        fromRef = "c1")

    compareDfs(
      actualDf.select(expectedHeader.head, expectedHeader.tail: _*),
      expectedDf.select(
        s"c1$$$labelCol", "c1Id", "c1Name", "c1Age", "c1Weight", "c1OnDiet",
        "elabel", "eId", s"$fromIdCol", s"$toIdCol", "since", "fights",
        s"c2Label", "c2Id", "c2Name", "c2Age", "c2Weight", "c2OnDiet"))
  }

  test("Binding table of Select - (c:Cat) WHERE c.weight > 4 AND c.onDiet = True") {
    val select =
      extractMatchClause("CONSTRUCT (c) MATCH (c:Cat) WHERE c.weight > 4 AND c.onDiet = True")
    val actualDf = sparkPlanner.solveBindingTable(select)

    val expectedHeader: Seq[String] =
      Seq(s"c$$$labelCol", s"c$$$idCol", "c$name", "c$age", "c$weight", "c$onDiet")
    compareHeaders(expectedHeader, actualDf)

    val expectedDf =
      Seq(hosico, maru).toDF.withColumn(TABLE_LABEL_COL.columnName, lit("Cat"))
    compareDfs(
      actualDf.select(s"c$$$labelCol", s"c$$$idCol", "c$name", "c$age", "c$weight", "c$onDiet"),
      expectedDf.select(labelCol, idCol, "name", "age", "weight", "onDiet"))
  }

  private def extractMatchClause(query: String): AlgebraTreeNode = {
    val createGraph = (parser andThen algebraRewriter) (query)
    createGraph.asInstanceOf[GraphCreate].matchClause
  }


  /**************************** Helpers to create test expectations. ******************************/
  private def createCatEatsFoodData(tuples: Seq[(Cat, Eats, Food)]): Seq[CatEatsFood] = {
    tuples
      .map(tuple => {
        val cat: Cat = tuple._1
        val eats: Eats = tuple._2
        val food: Food = tuple._3
        CatEatsFood(
          cat.id, cat.name, cat.age, cat.weight, cat.onDiet,
          food.id, food.brand,
          eats.id, eats.gramsPerDay, eats.fromId, eats.toId)
      })
  }

  private def createCatEatsFoodTable(tuples: Seq[(Cat, Eats, Food)],
                                     fromRef: String, edgeRef: String, toRef: String): DataFrame = {
    createCatEatsFoodData(tuples)
      .toDF
      .withColumn(s"$fromRef$$$labelCol", lit("Cat"))
      .withColumn(s"$edgeRef$$$labelCol", lit("Eats"))
      .withColumn(s"$toRef$$$labelCol", lit("Food"))
  }

  private def createCatFoodTable(tuples: Seq[(Cat, Food)],
                                 leftRef: String, rightRef: String): DataFrame = {
    tuples
      .map(tuple => {
        val cat: Cat = tuple._1
        val food: Food = tuple._2
        CatFood(cat.id, cat.name, cat.age, cat.weight, cat.onDiet, food.id, food.brand)
      })
      .toDF
      .withColumn(s"$rightRef$$$labelCol", lit("Cat"))
      .withColumn(s"$leftRef$$$labelCol", lit("Food"))
  }

  private
  def createFoodMadeInCountryTable(tuples: Seq[(Food, MadeIn, Country)],
                                   fromRef: String, edgeRef: String, toRef: String): DataFrame = {
    tuples
      .map(tuple => {
        val food: Food = tuple._1
        val madeIn: MadeIn = tuple._2
        val country: Country = tuple._3
        FoodMadeInCountry(
          food.id, food.brand, country.id, country.name, madeIn.id, madeIn.fromId, madeIn.toId)
      })
      .toDF
      .withColumn(s"$fromRef$$$labelCol", lit("Food"))
      .withColumn(s"$edgeRef$$$labelCol", lit("MadeIn"))
      .withColumn(s"$toRef$$$labelCol", lit("Country"))
  }

  private def createFoodCountryTable(tuples: Seq[(Food, Country)],
                                     lhsRef: String, rhsRef: String): DataFrame = {
    tuples
      .map(tuple => {
        val food: Food = tuple._1
        val country: Country = tuple._2
        FoodCountry(food.id, food.brand, country.id, country.name)
      })
      .toDF
      .withColumn(s"$lhsRef$$$labelCol", lit("Food"))
      .withColumn(s"$rhsRef$$$labelCol", lit("Country"))
  }

  private
  def createCatToGourmandFoodTable(tuples: Seq[(Cat, ToGourmand, Food, /*pathCost = */ Int)],
                                   fromRef: String, pathRef: String, toRef: String): DataFrame = {
    tuples
      .map(tuple => {
        val cat: Cat = tuple._1
        val toGourmand: ToGourmand = tuple._2
        val food: Food = tuple._3
        val pathCost: Int = tuple._4
        CatToGourmandFood(
          cat.id, cat.name, cat.age, cat.weight, cat.onDiet,
          food.id, food.brand,
          toGourmand.id, toGourmand.hops, toGourmand.fromId, toGourmand.toId, toGourmand.edges,
          pathCost)
      })
      .toDF
      .withColumn(s"$fromRef$$$labelCol", lit("Cat"))
      .withColumn(s"$pathRef$$$labelCol", lit("ToGourmand"))
      .withColumn(s"$toRef$$$labelCol", lit("Food"))
  }

  private
  def createEnemyUnionFriendTable(lhsTuples: Seq[(Cat, Friend, Cat)],
                                  rhsTuples: Seq[(Cat, Enemy, Cat)],
                                  fromRef: String, toRef: String): DataFrame = {
    /**
      * As one of the fields of the table (of type Int) will be null, we create the rows and the
      * schema separately, instead of with the help of a case class.
      */
    val lhsTable = lhsTuples
      .map(tuple => {
        val cat1: Cat = tuple._1
        val friend: Friend = tuple._2
        val cat2: Cat = tuple._3
        Row(
          cat1.id, cat1.name, cat1.age, cat1.weight, cat1.onDiet,
          cat2.id, cat2.name, cat2.age, cat2.weight, cat2.onDiet,
          friend.id, friend.fromId, friend.toId, friend.since, /*fights =*/ null,
          /*label =*/ "Friend")
      })
    val rhsTable = rhsTuples
      .map(tuple => {
        val cat1: Cat = tuple._1
        val enemy: Enemy = tuple._2
        val cat2: Cat = tuple._3
        Row(
          cat1.id, cat1.name, cat1.age, cat1.weight, cat1.onDiet,
          cat2.id, cat2.name, cat2.age, cat2.weight, cat2.onDiet,
          enemy.id, enemy.fromId, enemy.toId, enemy.since, enemy.fights, /*label = */ "Enemy")
      })

    val schema =
      List(
        StructField("c1Id", IntegerType, nullable = false),
        StructField("c1Name", StringType, nullable = false),
        StructField("c1Age", DoubleType, nullable = false),
        StructField("c1Weight", IntegerType, nullable = false),
        StructField("c1OnDiet", BooleanType, nullable = false),
        StructField("c2Id", IntegerType, nullable = false),
        StructField("c2Name", StringType, nullable = false),
        StructField("c2Age", DoubleType, nullable = false),
        StructField("c2Weight", IntegerType, nullable = false),
        StructField("c2OnDiet", BooleanType, nullable = false),
        StructField("eId", IntegerType, nullable = false),
        StructField("fromId", IntegerType, nullable = false),
        StructField("toId", IntegerType, nullable = false),
        StructField("since", StringType, nullable = false),
        StructField("fights", IntegerType, nullable = true),
        StructField("label", StringType, nullable = false)
      )

    spark.createDataFrame(spark.sparkContext.parallelize(lhsTable ++ rhsTable), StructType(schema))
      .withColumn(s"$fromRef$$$labelCol", lit("Cat"))
      .withColumn(s"$toRef$$$labelCol", lit("Cat"))
  }

  private def createCatEnemyCatTable(nullableTuples: Seq[Cat],
                                     matchedTuples: Seq[(Cat, Enemy, Cat)],
                                     fromRef: String): DataFrame = {
    val lhsTable = nullableTuples
      .map(cat => {
        Row(
          cat.id, cat.name, cat.age, cat.weight, cat.onDiet,
          /*rhs is null*/ null, null, null, null, null, /*c2label =*/ null,
          /*edge is null*/  null, null, null, null, null, /*elabel =*/ null)
      })
    val rhsTable = matchedTuples
      .map(tuple => {
        val cat1: Cat = tuple._1
        val enemy: Enemy = tuple._2
        val cat2: Cat = tuple._3
        Row(
          cat1.id, cat1.name, cat1.age, cat1.weight, cat1.onDiet,
          cat2.id, cat2.name, cat2.age, cat2.weight, cat2.onDiet, /*c2label =*/ "Cat",
          enemy.id, enemy.fromId, enemy.toId, enemy.since, enemy.fights, /*elabel = */ "Enemy")
      })

    val schema =
      List(
        StructField("c1Id", IntegerType, nullable = false),
        StructField("c1Name", StringType, nullable = false),
        StructField("c1Age", DoubleType, nullable = false),
        StructField("c1Weight", IntegerType, nullable = false),
        StructField("c1OnDiet", BooleanType, nullable = false),
        StructField("c2Id", IntegerType, nullable = true),
        StructField("c2Name", StringType, nullable = true),
        StructField("c2Age", DoubleType, nullable = true),
        StructField("c2Weight", IntegerType, nullable = true),
        StructField("c2OnDiet", BooleanType, nullable = true),
        StructField("c2Label", StringType, nullable = true),
        StructField("eId", IntegerType, nullable = true),
        StructField("fromId", IntegerType, nullable = true),
        StructField("toId", IntegerType, nullable = true),
        StructField("since", StringType, nullable = true),
        StructField("fights", IntegerType, nullable = true),
        StructField("elabel", StringType, nullable = true)
      )

    spark.createDataFrame(spark.sparkContext.parallelize(lhsTable ++ rhsTable), StructType(schema))
      .withColumn(s"$fromRef$$$labelCol", lit("Cat"))
  }
}
