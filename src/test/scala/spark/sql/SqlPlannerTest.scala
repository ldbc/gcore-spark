package spark.sql

import algebra.AlgebraRewriter
import algebra.operators.Column._
import algebra.operators._
import algebra.trees.{AlgebraContext, AlgebraTreeNode}
import compiler.CompileContext
import org.apache.spark.sql.functions.{expr, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import parser.SpoofaxParser
import parser.trees.ParseContext
import spark._

/**
  * Verifies that the [[SqlPlanner]] creates correct [[DataFrame]] binding tables. The tests
  * also assert that the implementation of the physical operators with Spark produces the expected
  * results. The operators are not tested individually - we only look at the results obtained by
  * running the code produced by these operators.
  */
class SqlPlannerTest extends FunSuite
  with TestGraph with BeforeAndAfterAll with SparkSessionTestWrapper {

  import spark.implicits._

  val db: SparkGraphDb = graphDb(spark)
  val graph: SparkGraph = catsGraph(spark)
  val sparkPlanner: SqlPlanner = SqlPlanner(CompileContext(db, spark))

  val parser: SpoofaxParser = SpoofaxParser(ParseContext(db))
  val algebraRewriter: AlgebraRewriter = AlgebraRewriter(AlgebraContext(db))

  val idCol: String = idColumn.columnName
  val fromIdCol: String = fromIdColumn.columnName
  val toIdCol: String = toIdColumn.columnName
  val labelCol: String = tableLabelColumn.columnName
  val edgesCol: String = edgeSeqColumn.columnName

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

  override def beforeAll() {
    super.beforeAll()
    db.registerGraph(graph)
    db.setDefaultGraph("cats graph")
  }

  /************************************ CONSTRUCT *************************************************/
  test("VertexCreate of bound variable - CONSTRUCT (c) MATCH (c)") {
    val vertex = extractConstructClauses("CONSTRUCT (c) MATCH (c)")
    val actualDf = sparkPlanner.constructGraph(bindingTable, vertex).head

    val expectedHeader: Seq[String] =
      Seq(s"c$$$labelCol", s"c$$$idCol", "c$name", "c$age", "c$weight", "c$onDiet")
    compareHeaders(expectedHeader, actualDf)

    val expectedDf =
      Seq(coby, hosico, maru, grumpy).toDF.withColumn(tableLabelColumn.columnName, lit("Cat"))
    compareDfs(
      actualDf.select(s"c$$$labelCol", s"c$$$idCol", "c$name", "c$age", "c$weight", "c$onDiet"),
      expectedDf.select(labelCol, idCol, "name", "age", "weight", "onDiet"))
  }

  test("VertexCreate of bound variable, new properties (expr, const, inline, SET) - " +
    "CONSTRUCT (c {dw := 2 * c.weight}) SET c.constInt := 1 MATCH (c)") {
    val vertex =
      extractConstructClauses("CONSTRUCT (c {dw := 2 * c.weight}) SET c.constInt := 1 MATCH (c)")
    val actualDf = sparkPlanner.constructGraph(bindingTable, vertex).head

    val existingProps: Seq[String] =
      Seq(s"c$$$labelCol", s"c$$$idCol", "c$name", "c$age", "c$weight", "c$onDiet")
    val newProps: Seq[String] = Seq("c$dw", "c$constInt")
    val expectedHeader: Seq[String] = existingProps ++ newProps

    compareHeaders(expectedHeader, actualDf)

    val expectedDf =
      Seq(coby, hosico, maru, grumpy).toDF
        .withColumn(tableLabelColumn.columnName, lit("Cat"))
        .withColumn("constInt", lit(1))
        .withColumn("dw", expr("2 * weight"))

    compareDfs(
      actualDf.select(
        s"c$$$labelCol", s"c$$$idCol", "c$name", "c$age", "c$weight", "c$onDiet",
        "c$dw", "c$constInt"),
      expectedDf.select(labelCol, idCol, "name", "age", "weight", "onDiet", "dw", "constInt"))
  }

  test("VertexCreate of bound variable, remove property and label - " +
    "CONSTRUCT (c) REMOVE c.onDiet REMOVE c:Cat MATCH (c)") {
    val vertex = extractConstructClauses("CONSTRUCT (c) REMOVE c.onDiet REMOVE c:Cat MATCH (c)")
    val actualDf = sparkPlanner.constructGraph(bindingTable, vertex).head

    val expectedHeader: Seq[String] = Seq(s"c$$$idCol", "c$name", "c$age", "c$weight")
    compareHeaders(expectedHeader, actualDf)

    val expectedDf = Seq(coby, hosico, maru, grumpy).toDF.drop("onDiet")
    compareDfs(
      actualDf.select(s"c$$$idCol", "c$name", "c$age", "c$weight"),
      expectedDf.select(idCol, "name", "age", "weight"))
  }

  test("VertexCreate of bound variable, filter binding table - " +
    "CONSTRUCT (c) WHEN c.age >= 5 MATCH (c)") {
    val vertex = extractConstructClauses("CONSTRUCT (c) WHEN c.age >= 5 MATCH (c)")
    val actualDf = sparkPlanner.constructGraph(bindingTable, vertex).head

    val expectedHeader: Seq[String] =
      Seq(s"c$$$labelCol", s"c$$$idCol", "c$name", "c$age", "c$weight", "c$onDiet")
    compareHeaders(expectedHeader, actualDf)

    val expectedDf = Seq(maru).toDF.withColumn(tableLabelColumn.columnName, lit("Cat"))
    compareDfs(
      actualDf.select(s"c$$$labelCol", s"c$$$idCol", "c$name", "c$age", "c$weight", "c$onDiet"),
      expectedDf.select(labelCol, idCol, "name", "age", "weight", "onDiet"))
  }

  test("VertexCreate of unbound variable - CONSTRUCT (x) MATCH (c)") {
    val vertex = extractConstructClauses("CONSTRUCT (x) MATCH (c)")
    val actualDf = sparkPlanner.constructGraph(bindingTable, vertex).head

    // Columns of c from the binding table are also preserved in the result in this case.
    val bindingTableColumns: Seq[String] =
      bindingTableSchema.fields
        .map(_.name)
        .filter(fieldName => fieldName.startsWith("c"))
    val expectedHeader: Seq[String] = Seq(s"x$$$idCol") ++ bindingTableColumns
    compareHeaders(expectedHeader, actualDf)

    // Cannot directly compare df's contents, because the monotonically increasing id's are not
    // necessarily contiguous numbers. We assert here that each new vertex receives a unique id.
    assert(
      actualDf.select(s"x$$$idCol").collect().map(_(0)).toSet.size ==
        bindingTableData.size)
  }

  test("VertexCreate of unbound variable, add prop and label - " +
    "CONSTRUCT (x :XLabel {constInt := 1}) MATCH (c)") {
    val vertex = extractConstructClauses("CONSTRUCT (x :XLabel {constInt := 1}) MATCH (c)")
    val actualDf = sparkPlanner.constructGraph(bindingTable, vertex).head

    // Columns of c from the binding table are also preserved in the result in this case.
    val bindingTableColumns: Seq[String] =
      bindingTableSchema.fields
        .map(_.name)
        .filter(fieldName => fieldName.startsWith("c"))
    val newColumns: Seq[String] = Seq(s"x$$$idCol", s"x$$$labelCol", "x$constInt")
    val expectedHeader: Seq[String] = bindingTableColumns ++ newColumns
    compareHeaders(expectedHeader, actualDf)

    val expectedDf =
      bindingTable
        .drop(tableLabelColumn.columnName)
        .withColumn(tableLabelColumn.columnName, lit("XLabel"))
        .withColumn("constInt", lit(1))
    compareDfs(
      actualDf.select(s"x$$$labelCol", "x$constInt"),
      expectedDf.select(labelCol, "constInt"))
  }

  test("VertexCreate of unbound variable, GROUP binding table - " +
    "CONSTRUCT (x GROUP c.onDiet) MATCH (c)") {
    val vertex = extractConstructClauses("CONSTRUCT (x GROUP c.onDiet) MATCH (c)")
    val actualDf = sparkPlanner.constructGraph(bindingTable, vertex).head

    // Only column onDiet of c from the binding table is preserved in the result in this case,
    // because it was used in an aggregation. This being a new variable with grouping, we will use
    // the grouping column for joining in a GroupConstruct. The other columns are removed in the
    // EntityConstruct.
    val expectedHeader: Seq[String] = Seq(s"x$$$idCol", "c$onDiet")
    compareHeaders(expectedHeader, actualDf)

    // Cannot directly compare df's contents, because the monotonically increasing id's are not
    // necessarily contiguous numbers. We assert here that for each group in the binding table
    // the new vertex x receives a unique id.
    assert(actualDf.collect().map(_(0)).toSet.size == bindingTableData.groupBy(_.onDiet).size)
  }

  test("VertexCreate of unbound variable, GROUP binding table, aggregate prop - " +
    "CONSTRUCT (x GROUP c.onDiet {avgw := AVG(c.weight)}) MATCH (c)") {
    val vertex =
      extractConstructClauses("CONSTRUCT (x GROUP c.onDiet {avgw := AVG(c.weight)}) MATCH (c)")
    val actualDf = sparkPlanner.constructGraph(bindingTable, vertex).head

    // Only column onDiet of c from the binding table is preserved in the result in this case,
    // because it was used in an aggregation. The other columns are removed in the EntityConstruct.
    val expectedHeader: Seq[String] = Seq(s"x$$$idCol", "x$avgw", "c$onDiet")
    compareHeaders(expectedHeader, actualDf)

    val expectedData =
      bindingTableData
        .groupBy(_.onDiet)
        .map {
          case (_, cats) => cats.map(_.weight).sum / cats.size.toDouble
        }
    val actualData =
      actualDf
        .select("x$avgw")
        .collect()
        .map(row => row(0))

    assert(expectedData.size == actualData.length)
    assert(expectedData.toSet == actualData.toSet)
  }

  test("EdgeCreate of bound edge and endpoints - CONSTRUCT (c)-[e]->(f) MATCH (c)-[e]->(f)") {
    val edge = extractConstructClauses("CONSTRUCT (c)-[e]->(f) MATCH (c)-[e]->(f)")
    val actualDf = sparkPlanner.constructGraph(bindingTable, edge).head

    // The binding table remains exactly the same as it is now. All of its fields and all of its
    // rows should be present in the result.
    val expectedHeader: Seq[String] = bindingTableSchema.fields.map(_.name)
    compareHeaders(expectedHeader, actualDf)

    val expectedDf = bindingTable
    compareDfs(
      actualDf.select(expectedHeader.head, expectedHeader.tail: _*),
      expectedDf.select(expectedHeader.head, expectedHeader.tail: _*))
  }

  test("EdgeCreate of bound edge, one bound and one unbound endpoint - " +
    "CONSTRUCT (c)-[e]->(b) MATCH (c)-[e]->(f)") {
    val edge = extractConstructClauses("CONSTRUCT (c)-[e]->(b) MATCH (c)-[e]->(f)")
    val actualDf = sparkPlanner.constructGraph(bindingTable, edge).head

    // All the attributes of the binding table are preserved in the result.
    val bindingTableColumns: Seq[String] = bindingTableSchema.fields.map(_.name)

    // + The new endpoint b, which only receives an id.
    val expectedHeader: Seq[String] = bindingTableColumns ++ Seq(s"b$$$idCol")
    compareHeaders(expectedHeader, actualDf)

    // First, compare the part of the binding table that stays constant.
    val expectedDf = bindingTable.select(bindingTableColumns.head, bindingTableColumns.tail: _*)
    compareDfs(
      actualDf.select(bindingTableColumns.head, bindingTableColumns.tail: _*),
      expectedDf)

    // Then, check that each new vertex b has received a different id. Previously, f had 3 distinct
    // id's, now we expect b to have 4 distinct id's, because it was an unbound variable, so we
    // create one vertex for each matched row in the binding table.
    val bids = actualDf.select(s"b$$$idCol").collect().map(_(0))
    assert(bids.toSet.size == bindingTableRows.length) // no 2 ids are equal
  }

  test("EdgeCreate of bound edge, two unbound endpoints - " +
    "CONSTRUCT (a)-[e]->(b) MATCH (c)-[e]->(f)") {
    val edge = extractConstructClauses("CONSTRUCT (a)-[e]->(b) MATCH (c)-[e]->(f)")
    val actualDf = sparkPlanner.constructGraph(bindingTable, edge).head

    // All the attributes of the binding table are preserved in the result.
    val bindingTableColumns: Seq[String] = bindingTableSchema.fields.map(_.name)

    // + The two new endpoints a and b, which only receive an id.
    val expectedHeader: Seq[String] = bindingTableColumns ++ Seq(s"a$$$idCol", s"b$$$idCol")
    compareHeaders(expectedHeader, actualDf)

    // First, compare the part of the binding table that stays constant.
    val expectedDf = bindingTable.select(bindingTableColumns.head, bindingTableColumns.tail: _*)
    compareDfs(
      actualDf.select(bindingTableColumns.head, bindingTableColumns.tail: _*),
      expectedDf)

    // Then, check that each new vertex a or b has received a different id.
    val aids = actualDf.select(s"a$$$idCol").collect().map(_(0))
    assert(aids.toSet.size == bindingTableRows.length) // no 2 ids are equal

    val bids = actualDf.select(s"b$$$idCol").collect().map(_(0))
    assert(bids.toSet.size == bindingTableRows.length) // no 2 ids are equal
  }

  test("EdgeCreate of unbound edge, two bound endpoints - " +
    "CONSTRUCT (c)-[x]-(f) MATCH (c)-[e]->(f)") {
    val edge = extractConstructClauses("CONSTRUCT (c)-[x]-(f) MATCH (c)-[e]->(f)")
    val actualDf = sparkPlanner.constructGraph(bindingTable, edge).head

    // All attributes of the binding table are preserved.
    val bindingTableColumns: Seq[String] = bindingTableSchema.fields.map(_.name)

    // + The new edge x, which only receives an id.
    val expectedHeader: Seq[String] = bindingTableColumns ++ Seq(s"x$$$idCol")
    compareHeaders(expectedHeader, actualDf)

    // First, compare the part of the binding table that stays constant.
    val expectedDf = bindingTable.select(bindingTableColumns.head, bindingTableColumns.tail: _*)
    compareDfs(
      actualDf.select(bindingTableColumns.head, bindingTableColumns.tail: _*),
      expectedDf)

    // Then, check that each new edge x has received a different id.
    val xids = actualDf.select(s"x$$$idCol").collect().map(_(0))
    assert(xids.toSet.size == bindingTableRows.length) // no 2 ids are equal
  }

  test("EdgeCreate of unbound edge, two unbound endpoints - " +
    "CONSTRUCT (a)-[x]-(b) MATCH (c)-[e]->(f)") {
    val edge = extractConstructClauses("CONSTRUCT (a)-[x]-(b) MATCH (c)-[e]->(f)")
    val actualDf = sparkPlanner.constructGraph(bindingTable, edge).head

    // All attributes of the binding table are preserved.
    val bindingTableColumns: Seq[String] = bindingTableSchema.fields.map(_.name)

    // + The new edge x and endpoints a and b, which only receive an id.
    val expectedHeader: Seq[String] =
      bindingTableColumns ++ Seq(s"x$$$idCol", s"a$$$idCol", s"b$$$idCol")
    compareHeaders(expectedHeader, actualDf)

    // Compare the part of the binding table that stays constant.
    val expectedDf = bindingTable.select(bindingTableColumns.head, bindingTableColumns.tail: _*)
    compareDfs(
      actualDf.select(bindingTableColumns.head, bindingTableColumns.tail: _*),
      expectedDf)

    // Check that each new edge x or vertex a or b has received a different id.
    val xids = actualDf.select(s"x$$$idCol").collect().map(_(0))
    assert(xids.toSet.size == bindingTableRows.length) // no 2 ids are equal

    val aids = actualDf.select(s"a$$$idCol").collect().map(_(0))
    assert(aids.toSet.size == bindingTableRows.length) // no 2 ids are equal

    val bids = actualDf.select(s"b$$$idCol").collect().map(_(0))
    assert(bids.toSet.size == bindingTableRows.length) // no 2 ids are equal
  }

  test("EdgeCreate of unbound edge, one bound endpoint, one unbound grouped endpoint - " +
    "CONSTRUCT (c)-[x]->(d GROUP c.onDiet) MATCH (c)-[e]->(f)") {
    val edge = extractConstructClauses("CONSTRUCT (c)-[x]->(d GROUP c.onDiet) MATCH (c)-[e]->(f)")
    val actualDf = sparkPlanner.constructGraph(bindingTable, edge).head

    // All attributes of the binding table are preserved.
    val bindingTableColumns: Seq[String] = bindingTableSchema.fields.map(_.name)

    // x and d each only receive an id.
    val expectedHeader: Seq[String] = bindingTableColumns ++ Seq(s"x$$$idCol", s"d$$$idCol")
    compareHeaders(expectedHeader, actualDf)

    // Compare the part of the binding table that stays constant.
    val expectedDf = bindingTable.select(bindingTableColumns.head, bindingTableColumns.tail: _*)
    compareDfs(
      actualDf.select(bindingTableColumns.head, bindingTableColumns.tail: _*),
      expectedDf)

    // Check that each new edge x received a different id.
    val xids = actualDf.select(s"x$$$idCol").collect().map(_(0))
    assert(xids.toSet.size == bindingTableRows.length) // no 2 ids are equal

    // Check that each new vertex d received as many new id's, as there are groups of c.onDiet in
    // the binding table.
    val dids = actualDf.select(s"d$$$idCol").collect().map(_(0))
    assert(dids.toSet.size == bindingTableData.groupBy(_.onDiet).size)
  }

  test("EdgeCreate with new properties and labels for endpoints and connection - " +
    "CONSTRUCT (c)-[x :OnDiet]->(d GROUP c.onDiet :Boolean {val := c.onDiet}) " +
    "MATCH (c)-[e]->(f)") {
    val edge =
      extractConstructClauses(
        "CONSTRUCT (c)-[x :OnDiet]->(d GROUP c.onDiet :Boolean {val := c.onDiet}) " +
          "MATCH (c)-[e]->(f)")
    val actualDf = sparkPlanner.constructGraph(bindingTable, edge).head

    // All attributes of the binding table are preserved.
    val bindingTableColumns: Seq[String] = bindingTableSchema.fields.map(_.name)

    // edge x receives a new labels and an id attribute
    val xAttributes = Seq(s"x$$$idCol", s"x$$$labelCol")

    // vertex d receives a new label, property val and an id attribute
    val dAttributes = Seq(s"d$$$idCol", s"d$$$labelCol", "d$val")

    // x and d each only receive an id.
    val expectedHeader: Seq[String] = bindingTableColumns ++ xAttributes ++ dAttributes
    compareHeaders(expectedHeader, actualDf)

    // Omit the new id columns, because we check the ids are correct in a previous test. We are now
    // interested in testing the labels and properties only.
    val expectedDf =
      bindingTable
        .withColumn(s"x$$$labelCol", lit("OnDiet"))
        .withColumn(s"d$$$labelCol", lit("Boolean"))
        .withColumn("d$val", expr("`c$onDiet`"))

    compareDfs(
      actualDf.select(s"x$$$labelCol", s"d$$$labelCol", "d$val"),
      expectedDf.select(s"x$$$labelCol", s"d$$$labelCol", "d$val"))
  }

  test("EdgeCreate from duplicate pairs of endpoints, check implicit grouping is used - " +
    "CONSTRUCT (c1)-[e0]->(f1) MATCH (c1)-[e1]->(f1), (c2)-[e2]->(f2) (cross-join of patterns)") {
    val edge =
      extractConstructClauses("CONSTRUCT (c1)-[e0]->(f1) MATCH (c1)-[e1]->(f1), (c2)-[e2]->(f2)")
    val actualDf = sparkPlanner.constructGraph(bindingTableDuplicateData, edge).head

    // All attributes of the binding table are preserved.
    val bindingTableColumns: Seq[String] = bindingTableDuplicateData.schema.fields.map(_.name)

    // Edge e0 receives an id.
    val expectedHeader: Seq[String] = bindingTableColumns ++ Seq(s"e0$$$idCol")
    compareHeaders(expectedHeader, actualDf)

    // The number of new edges e0 must be equal to the number of unique pairs (c1, f1).
    val e0ids = actualDf.select(s"e0$$$idCol").collect().map(_(0))
    val vertexGroups =
      bindingTableDuplicateData
        .select(s"c1$$$idCol", s"f1$$$idCol")
        .collect()
        .map(row => (row(0).toString, row(1).toString))
        .toSet
    assert(e0ids.toSet.size == vertexGroups.size)
  }

  test("GroupConstruct of bound endpoints, unbound edges - " +
    "CONSTRUCT (c)-[e0]->(f)-[e1]->(c) MATCH (c)-[e]->(f)") {
    val group = extractConstructClauses("CONSTRUCT (c)-[e0]->(f)-[e1]->(c) MATCH (c)-[e]->(f)")
    val actualDf = sparkPlanner.constructGraph(bindingTable, group).head

    // All columns from the binding table are preserved + the new columns for edges e0 and e1.
    val bindingTableColumns: Seq[String] = bindingTableSchema.fields.map(_.name)
    val expectedHeader: Seq[String] = bindingTableColumns ++ Seq(s"e0$$$idCol", s"e1$$$idCol")
    compareHeaders(expectedHeader, actualDf)

    // Compare the part of the binding table that remains constant.
    compareDfs(
      actualDf.select(bindingTableColumns.head, bindingTableColumns.tail: _*),
      bindingTable.select(bindingTableColumns.head, bindingTableColumns.tail: _*))

    // Check that each new edge receives a unique id.
    val e0ids = actualDf.select(s"e0$$$idCol").collect().map(_(0))
    assert(e0ids.toSet.size == bindingTableData.size)

    val e1ids = actualDf.select(s"e1$$$idCol").collect().map(_(0))
    assert(e1ids.toSet.size == bindingTableData.size)
  }

  test("GroupConstruct, add one vertex between matched endpoints - " +
    "CONSTRUCT (c)-[e0]->(x)-[e1]->(f) MATCH (c)-[e]->(f)") {
    val group = extractConstructClauses("CONSTRUCT (c)-[e0]->(x)-[e1]->(f) MATCH (c)-[e]->(f)")
    val actualDf = sparkPlanner.constructGraph(bindingTable, group).head

    // All columns from the binding table are preserved + the new columns for edges e0 and e1 and
    // vertex x.
    val bindingTableColumns: Seq[String] = bindingTableSchema.fields.map(_.name)
    val expectedHeader: Seq[String] =
      bindingTableColumns ++ Seq(s"e0$$$idCol", s"e1$$$idCol", s"x$$$idCol")
    compareHeaders(expectedHeader, actualDf)

    // Skip common binding table comparison, as it was tested before. Only check that each new edge
    // and the new vertex receive a unique id.
    val e0ids = actualDf.select(s"e0$$$idCol").collect().map(_(0))
    assert(e0ids.toSet.size == bindingTableData.size)

    val e1ids = actualDf.select(s"e1$$$idCol").collect().map(_(0))
    assert(e1ids.toSet.size == bindingTableData.size)

    val xids = actualDf.select(s"x$$$idCol").collect().map(_(0))
    assert(xids.toSet.size == bindingTableData.size)
  }

  test("GroupConstruct with GROUP-ing - " +
    "CONSTRUCT (d GROUP c.onDiet)<-(c)->(f) MATCH (c)-[e]->(f)") {
    val group =
      extractConstructClauses("CONSTRUCT (d GROUP c.onDiet)<-[e0]-(c)-[e1]->(f) MATCH (c)-[e]->(f)")
    val actualDf = sparkPlanner.constructGraph(bindingTable, group).head

    // All columns from the binding table are preserved + the new columns for edges e0 and e1 and
    // vertex d.
    val bindingTableColumns: Seq[String] = bindingTableSchema.fields.map(_.name)
    val expectedHeader: Seq[String] =
      bindingTableColumns ++ Seq(s"e0$$$idCol", s"e1$$$idCol", s"d$$$idCol")
    compareHeaders(expectedHeader, actualDf)

    // Check that each new edge received a unique id.
    val e0ids = actualDf.select(s"e0$$$idCol").collect().map(_(0))
    assert(e0ids.toSet.size == bindingTableData.size)

    val e1ids = actualDf.select(s"e1$$$idCol").collect().map(_(0))
    assert(e1ids.toSet.size == bindingTableData.size)

    // Check that vertex d only received as many ids as there are groupings by c.onDiet.
    val xids = actualDf.select(s"d$$$idCol").collect().map(_(0))
    assert(xids.toSet.size == bindingTableData.groupBy(_.onDiet).size)
  }

  test("GroupConstruct of two vertices, with filtering - " +
    "CONSTRUCT (c) WHEN c.age <= 3, (c) WHEN c.age > 3 MATCH (c)") {
    val group =
      extractConstructClauses(
        query = "CONSTRUCT (c) WHEN c.age <= 3, (c) WHEN c.age > 3 MATCH (c)",
        expectedNumClauses = 2)
    val actualDfs = sparkPlanner.constructGraph(bindingTable, group)

    // Check that the two tables have the correct elements.
    val bindingTableColumns: Seq[String] =
      bindingTableSchema.fields.map(_.name).filter(_.startsWith("c"))
    val expectedBelowThree = bindingTable.select("*").where("`c$age` <= 3")
    val expectedAboveThree = bindingTable.select("*").where("`c$age` > 3")

    compareDfs(
      actualDfs.head.select(bindingTableColumns.head, bindingTableColumns.tail: _*),
      expectedBelowThree.select(bindingTableColumns.head, bindingTableColumns.tail: _*))
    compareDfs(
      actualDfs.last.select(bindingTableColumns.head, bindingTableColumns.tail: _*),
      expectedAboveThree.select(bindingTableColumns.head, bindingTableColumns.tail: _*))
  }

  private def extractConstructClauses(query: String, expectedNumClauses: Int = 1)
  : Seq[AlgebraTreeNode] = {

    val createGraph = (parser andThen algebraRewriter) (query)
    val constructClauses = createGraph.asInstanceOf[GraphCreate].constructClauses

    assert(constructClauses.size == expectedNumClauses)

    constructClauses
  }

  /************************************** MATCH ***************************************************/
  test("Binding table of VertexScan - (c:Cat)") {
    val scan = extractMatchClause("CONSTRUCT (c) MATCH (c:Cat)")
    val actualDf = sparkPlanner.solveBindingTable(scan)

    val expectedHeader: Seq[String] =
      Seq(s"c$$$labelCol", s"c$$$idCol", "c$name", "c$age", "c$weight", "c$onDiet")
    compareHeaders(expectedHeader, actualDf)

    val expectedDf =
      Seq(coby, hosico, maru, grumpy).toDF.withColumn(tableLabelColumn.columnName, lit("Cat"))
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
      * (path length = number of edges in the [[edgeSeqColumn]] of the path.
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
      * [[tableLabelColumn]], [[idColumn]], [[fromIdColumn]], [[toIdColumn]] and "since" are common
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
      Seq(hosico, maru).toDF.withColumn(tableLabelColumn.columnName, lit("Cat"))
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
