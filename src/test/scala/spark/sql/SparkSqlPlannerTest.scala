package spark.sql

import algebra.expressions._
import algebra.operators._
import algebra.trees.AlgebraTreeNode
import algebra.trees.CreateGroupingSets._
import algebra.types.{DefaultGraph, GroupDeclaration}
import org.apache.spark.sql.functions.{expr, lit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import planner.operators.Column._
import planner.operators._
import planner.trees.{AlgebraToPlannerTree, PlannerContext}
import spark._

import scala.collection.immutable

/**
  * Verifies that the [[SparkSqlPlanner]] creates correct [[DataFrame]] binding tables. The tests
  * also assert that the implementation of the physical operators with Spark produces the expected
  * results. The operators are not tested individually - we only look at the results obtained by
  * running the code produced by these operators.
  */
class SparkSqlPlannerTest extends FunSuite
  with TestGraph with BeforeAndAfterAll with SparkSessionTestWrapper {

  import spark.implicits._

  val db: SparkGraphDb = graphDb(spark)
  val graph: SparkGraph = catsGraph(spark)
  val sparkPlanner: SparkSqlPlanner = SparkSqlPlanner(spark)

  val algebraRewriter: AlgebraToPlannerTree = AlgebraToPlannerTree(PlannerContext(db))

  val idCol: String = idColumn.columnName
  val fromIdCol: String = fromIdColumn.columnName
  val toIdCol: String = toIdColumn.columnName
  val labelCol: String = tableLabelColumn.columnName
  val edgesCol: String = edgeSeqColumn.columnName

  /** MATCH (c:Cat) */
  val bindingTableSchema: StructType =
    StructType(List(
      StructField(s"c$$$idCol", IntegerType, nullable = false),
      StructField(s"c$$$labelCol", StringType, nullable = false),
      StructField("c$name", StringType, nullable = false),
      StructField("c$age", DoubleType, nullable = false),
      StructField("c$weight", IntegerType, nullable = false),
      StructField("c$onDiet", BooleanType, nullable = false)
    ))
  val bindingTableData: Seq[Cat] = Seq(coby, hosico, maru, grumpy)
  val bindingTableRows: Seq[Row] =
    bindingTableData.map {
      case Cat(id, name, age, weight, onDiet) => Row(id, "Cat", name, age, weight, onDiet)
    }
  val bindingTable: DataFrame =
    spark.createDataFrame(
      spark.sparkContext.parallelize(bindingTableRows),
      bindingTableSchema)

  override def beforeAll() {
    super.beforeAll()
    db.registerGraph(graph)
    db.setDefaultGraph("cats graph")
  }

  /************************************ CONSTRUCT *************************************************/
  test("VertexCreate of bound variable - CONSTRUCT (c) MATCH (c)") {
    val vertex =
      vertexCreate(
        reference = Reference("c"),
        objConstructPattern = ObjectConstructPattern(True, True),
        groupingAttributes = Seq(Reference("c")),
        projectAttributes = Set(Reference("c")),
        aggregateFunctions = Seq.empty,
        when = True,
        setClause =None,
        removeClause = None)
    val actualDf = sparkPlanner.constructGraph(bindingTable, Seq(vertex)).head

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
      vertexCreate(
        reference = Reference("c"),
        objConstructPattern =
          ObjectConstructPattern(
            labelAssignments = True,
            propAssignments =
              PropAssignments(Seq(
                PropAssignment(
                  PropertyKey("dw"),
                  Mul(IntLiteral(2), PropertyRef(Reference("c"), PropertyKey("weight"))))))
          ),
        groupingAttributes = Seq(Reference("c")),
        projectAttributes = Set(Reference("c")),
        aggregateFunctions = Seq.empty,
        when = True,
        setClause =
          Some(
            SetClause(Seq(
              PropertySet(Reference("c"), PropAssignment(PropertyKey("constInt"), IntLiteral(1))))
            )
          ),
        removeClause = None)
    val actualDf = sparkPlanner.constructGraph(bindingTable, Seq(vertex)).head

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
    val vertex =
      vertexCreate(
        reference = Reference("c"),
        objConstructPattern = ObjectConstructPattern(True, True),
        groupingAttributes = Seq(Reference("c")),
        projectAttributes = Set(Reference("c")),
        aggregateFunctions = Seq.empty,
        when = True,
        setClause =None,
        removeClause =
          Some(
            RemoveClause(
              propRemoves = Seq(PropertyRemove(PropertyRef(Reference("c"), PropertyKey("onDiet")))),
              labelRemoves = Seq(LabelRemove(Reference("c"), LabelAssignments(Seq(Label("Cat")))))
            )
          )
      )
    val actualDf = sparkPlanner.constructGraph(bindingTable, Seq(vertex)).head

    val expectedHeader: Seq[String] = Seq(s"c$$$idCol", "c$name", "c$age", "c$weight")
    compareHeaders(expectedHeader, actualDf)

    val expectedDf = Seq(coby, hosico, maru, grumpy).toDF.drop("onDiet")
    compareDfs(
      actualDf.select(s"c$$$idCol", "c$name", "c$age", "c$weight"),
      expectedDf.select(idCol, "name", "age", "weight"))
  }

  test("VertexCreate of bound variable, filter binding table - " +
    "CONSTRUCT (c) WHEN c.age >= 5 MATCH (c)") {
    val vertex =
      vertexCreate(
        reference = Reference("c"),
        objConstructPattern = ObjectConstructPattern(True, True),
        groupingAttributes = Seq(Reference("c")),
        projectAttributes = Set(Reference("c")),
        aggregateFunctions = Seq.empty,
        when =
          Gt(
            PropertyRef(Reference("c"), PropertyKey("age")),
            IntLiteral(5)),
        setClause = None,
        removeClause = None)
    val actualDf = sparkPlanner.constructGraph(bindingTable, Seq(vertex)).head

    val expectedHeader: Seq[String] =
      Seq(s"c$$$labelCol", s"c$$$idCol", "c$name", "c$age", "c$weight", "c$onDiet")
    compareHeaders(expectedHeader, actualDf)

    val expectedDf = Seq(maru).toDF.withColumn(tableLabelColumn.columnName, lit("Cat"))
    compareDfs(
      actualDf.select(s"c$$$labelCol", s"c$$$idCol", "c$name", "c$age", "c$weight", "c$onDiet"),
      expectedDf.select(labelCol, idCol, "name", "age", "weight", "onDiet"))
  }

  test("VertexCreate of unbound variable - CONSTRUCT (x) MATCH (c)") {
    val vertex =
      vertexCreate(
        reference = Reference("x"),
        objConstructPattern = ObjectConstructPattern(True, True),
        groupingAttributes = Seq.empty,
        projectAttributes = Set(Reference("x")),
        aggregateFunctions = Seq.empty,
        when = True,
        setClause =None,
        removeClause = None)
    val actualDf = sparkPlanner.constructGraph(bindingTable, Seq(vertex)).head

    val expectedHeader: Seq[String] = Seq(s"x$$$idCol")
    compareHeaders(expectedHeader, actualDf)

    // Cannot directly compare df's contents, because the monotonically increasing id's are not
    // necessarily contiguous numbers.
    assert(actualDf.collect().length == bindingTableData.size)
  }

  test("VertexCreate of unbound variable, add prop and label - " +
    "CONSTRUCT (x :XLabel {constInt := 1}) MATCH (c)") {
    val vertex =
      vertexCreate(
        reference = Reference("x"),
        objConstructPattern =
          ObjectConstructPattern(
            labelAssignments = LabelAssignments(Seq(Label("XLabel"))),
            propAssignments =
              PropAssignments(Seq(
                PropAssignment(
                  PropertyKey("constInt"),
                  IntLiteral(1))))
          ),
        groupingAttributes = Seq.empty,
        projectAttributes = Set(Reference("x")),
        aggregateFunctions = Seq.empty,
        when = True,
        setClause = None,
        removeClause = None)
    val actualDf = sparkPlanner.constructGraph(bindingTable, Seq(vertex)).head

    val expectedHeader: Seq[String] = Seq(s"x$$$idCol", s"x$$$labelCol", "x$constInt")
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
    val vertex =
      vertexCreate(
        reference = Reference("x"),
        objConstructPattern = ObjectConstructPattern(True, True),
        groupingAttributes = Seq(
          GroupDeclaration(
            groupingSets = Seq(PropertyRef(Reference("c"), PropertyKey("onDiet")))
          )),
        projectAttributes = Set(Reference("x")),
        aggregateFunctions = Seq.empty,
        when = True,
        setClause =None,
        removeClause = None)
    val actualDf = sparkPlanner.constructGraph(bindingTable, Seq(vertex)).head

    val expectedHeader: Seq[String] = Seq(s"x$$$idCol")
    compareHeaders(expectedHeader, actualDf)

    // Cannot directly compare df's contents, because the monotonically increasing id's are not
    // necessarily contiguous numbers.
    assert(actualDf.collect().length == bindingTableData.groupBy(_.onDiet).size)
  }

  test("VertexCreate of unbound variable, GROUP binding table, aggregate prop - " +
    "CONSTRUCT (x GROUP c.onDiet {avgw := AVG(c.weight)}) MATCH (c)") {
    val aggPropName = s"$PROP_AGG_BASENAME-AVG"
    val vertex =
      vertexCreate(
        reference = Reference("x"),
        objConstructPattern =
          ObjectConstructPattern(
            labelAssignments = True,
            propAssignments =
              PropAssignments(Seq(
                PropAssignment(
                  PropertyKey("avgw"),
                  PropertyRef(Reference("x"), PropertyKey(aggPropName)))))
          ),
        groupingAttributes = Seq(
          GroupDeclaration(
            groupingSets = Seq(PropertyRef(Reference("c"), PropertyKey("onDiet")))
          )),
        projectAttributes = Set(Reference("x")),
        aggregateFunctions = Seq(
          PropertySet(
            ref = Reference("x"),
            propAssignment =
              PropAssignment(
                PropertyKey(aggPropName),
                Avg(distinct = false, PropertyRef(Reference("c"), PropertyKey("weight")))
              ))
        ),
        when = True,
        setClause = None,
        removeClause =
          Some(
            RemoveClause(
              propRemoves = Seq(
                PropertyRemove(PropertyRef(Reference("x"), PropertyKey(aggPropName)))),
              labelRemoves = Seq.empty))
      )
    val actualDf = sparkPlanner.constructGraph(bindingTable, Seq(vertex)).head

    val expectedHeader: Seq[String] = Seq(s"x$$$idCol", "x$avgw")
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

  private def vertexCreate(reference: Reference,
                           objConstructPattern: ObjectConstructPattern,
                           groupingAttributes: Seq[AlgebraTreeNode],
                           projectAttributes: Set[Reference],
                           aggregateFunctions: Seq[PropertySet],
                           when: AlgebraExpression,
                           setClause: Option[SetClause],
                           removeClause: Option[RemoveClause]): VertexCreate = {
    val vertexConstructRelation =
      VertexConstructRelation(
        reference,
        relation =
          processBindingTable(
            reference, BindingTable(new BindingSet(Reference("c"))), when,
            groupingAttributes, aggregateFunctions, projectAttributes),
        expr = objConstructPattern,
        setClause = setClause,
        removeClause = removeClause)
    (algebraRewriter rewriteTree vertexConstructRelation).asInstanceOf[VertexCreate]
  }

  /************************************** MATCH ***************************************************/
  test("Binding table of VertexScan - (c:Cat)") {
    val scan =
      VertexScan(
        VertexRelation(Reference("c"), Relation(Label("Cat")), True),
        DefaultGraph, PlannerContext(db))
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
    val scan =
      EdgeScan(
        EdgeRelation(
          Reference("e"), Relation(Label("Eats")), expr = True,
          fromRel = VertexRelation(Reference("c"), Relation(Label("Cat")), True),
          toRel = VertexRelation(Reference("f"), Relation(Label("Food")), True)),
        DefaultGraph, PlannerContext(db))
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
      actualDf.select(
        s"c$$$labelCol", s"c$$$idCol", "c$name", "c$age", "c$weight", "c$onDiet",
        s"e$$$labelCol", s"e$$$idCol", s"e$$$fromIdCol", s"e$$$toIdCol", "e$gramsPerDay",
        s"f$$$labelCol", s"f$$$idCol", "f$brand"),
      expectedDf.select(
        s"c$$$labelCol", "catId", "name", "age", "weight", "onDiet",
        s"e$$$labelCol", "eatsId", s"$fromIdCol", s"$toIdCol", "gramsPerDay",
        s"f$$$labelCol", "foodId", "brand"))
  }

  test("Binding table of PathScan, isReachableTest = true - (c:Cat)-/@p/->(f:Food)") {
    val scan =
      PathScan(
        StoredPathRelation(
          Reference("p"), isReachableTest = true, Relation(Label("ToGourmand")), expr = True,
          fromRel = VertexRelation(Reference("c"), Relation(Label("Cat")), True),
          toRel = VertexRelation(Reference("f"), Relation(Label("Food")), True),
          costVarDef = None, quantifier = None),
        DefaultGraph, PlannerContext(db))
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
      actualDf.select(
        s"c$$$labelCol", s"c$$$idCol", "c$name", "c$age", "c$weight", "c$onDiet",
        s"f$$$labelCol", s"f$$$idCol", "f$brand"),
      expectedDf.select(
        s"c$$$labelCol", "catId", "name", "age", "weight", "onDiet",
        s"f$$$labelCol", "foodId", "brand"))
  }

  test("Binding table of PathScan, isReachableTest = false, costVarDef = cost - " +
    "(c:Cat)-/@p COST cost/->(f:Food)") {
    val scan =
      PathScan(
        StoredPathRelation(
          Reference("p"), isReachableTest = false, Relation(Label("ToGourmand")), expr = True,
          fromRel = VertexRelation(Reference("c"), Relation(Label("Cat")), True),
          toRel = VertexRelation(Reference("f"), Relation(Label("Food")), True),
          costVarDef = Some(Reference("cost")), quantifier = None),
        DefaultGraph, PlannerContext(db))
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
      actualDf.select(
        s"c$$$labelCol", s"c$$$idCol", "c$name", "c$age", "c$weight", "c$onDiet",
        s"p$$$labelCol", s"p$$$idCol", s"p$$$fromIdCol", s"p$$$toIdCol", s"p$$$edgesCol",
        "p$hops", "p$cost",
        s"f$$$labelCol", s"f$$$idCol", "f$brand"),
      expectedDf.select(
        s"c$$$labelCol", "catId", "name", "age", "weight", "onDiet",
        s"p$$$labelCol", "toGourmandId", s"$fromIdCol", s"$toIdCol", s"$edgesCol", "hops", "cost",
        s"f$$$labelCol", "foodId", "brand"))
  }

  test("Binding table of UnionAll - (c1:Cat)->(c2:Cat). Each side of the union is padded with " +
    "missing columns, set to null.") {
    val lhs =
      EdgeRelation(
        Reference("e"), Relation(Label("Friend")), expr = True,
        fromRel = VertexRelation(Reference("c1"), Relation(Label("Cat")), True),
        toRel = VertexRelation(Reference("c2"), Relation(Label("Cat")), True))
    val rhs =
      EdgeRelation(
        Reference("e"), Relation(Label("Enemy")), expr = True,
        fromRel = VertexRelation(Reference("c1"), Relation(Label("Cat")), True),
        toRel = VertexRelation(Reference("c2"), Relation(Label("Cat")), True))
    val edgeScan1 = EdgeScan(lhs, DefaultGraph, PlannerContext(db))
    val edgeScan2 = EdgeScan(rhs, DefaultGraph, PlannerContext(db))
    val union = UnionAll(lhs, rhs)
    union.children = List(edgeScan1, edgeScan2)
    val actualDf = sparkPlanner.solveBindingTable(BindingTableOp(union))

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
      actualDf.select(
        s"c1$$$labelCol", s"c1$$$idCol", "c1$name", "c1$age", "c1$weight", "c1$onDiet",
        s"e$$$labelCol", s"e$$$idCol", s"e$$$fromIdCol", s"e$$$toIdCol", "e$since", "e$fights",
        s"c2$$$labelCol", s"c2$$$idCol", "c2$name", "c2$age", "c2$weight", "c2$onDiet"),
      expectedDf.select(
        s"c1$$$labelCol", "c1Id", "c1Name", "c1Age", "c1Weight", "c1OnDiet",
        "label", "eId", s"$fromIdCol", s"$toIdCol", "since", "fights",
        s"c2$$$labelCol", "c2Id", "c2Name", "c2Age", "c2Weight", "c2OnDiet"))
  }

  test("Binding table of InnerJoin - (f:Food)->(c:Country), (f). Common columns used in the join " +
    "are correctly identified.") {
    val lhs =
      EdgeRelation(
        Reference("e"), Relation(Label("MadeIn")), expr = True,
        fromRel = VertexRelation(Reference("f"), Relation(Label("Food")), True),
        toRel = VertexRelation(Reference("c"), Relation(Label("Country")), True))
    val rhs = VertexRelation(Reference("f"), Relation(Label("Food")), expr = True)
    val edgeScan = EdgeScan(lhs, DefaultGraph, PlannerContext(db))
    val vertexScan = VertexScan(rhs, DefaultGraph, PlannerContext(db))
    val join = InnerJoin(lhs, rhs)
    join.children = List(edgeScan, vertexScan)
    val actualDf = sparkPlanner.solveBindingTable(BindingTableOp(join))

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
      actualDf.select(
        s"f$$$labelCol", s"f$$$idCol", "f$brand",
        s"e$$$labelCol", s"e$$$idCol", s"e$$$fromIdCol", s"e$$$toIdCol",
        s"c$$$labelCol", s"c$$$idCol", "c$name"),
      expectedDf.select(
        s"f$$$labelCol", "foodId", "brand",
        s"e$$$labelCol", "madeInId", s"$fromIdCol", s"$toIdCol",
        s"c$$$labelCol", "countryId", "name"))
  }

  test("Binding table of CrossJoin - (f:Food), (c:Country)") {
    val lhs = VertexRelation(Reference("f"), Relation(Label("Food")), expr = True)
    val rhs = VertexRelation(Reference("c"), Relation(Label("Country")), expr = True)
    val lhsScan = VertexScan(lhs, DefaultGraph, PlannerContext(db))
    val rhsScan = VertexScan(rhs, DefaultGraph, PlannerContext(db))
    val join = CrossJoin(lhs, rhs)
    join.children = List(lhsScan, rhsScan)
    val actualDf = sparkPlanner.solveBindingTable(BindingTableOp(join))

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
      actualDf.select(
        s"f$$$labelCol", s"f$$$idCol", "f$brand", s"c$$$labelCol", s"c$$$idCol", "c$name"),
      expectedDf.select(s"f$$$labelCol", "foodId", "brand", s"c$$$labelCol", "countryId", "name"))
  }

  test("Binding table of LeftOuterJoin - (c1:Cat), (c1)-[:Enemy]->(c2). Common columns used in " +
    "the join are correctly identified.") {
    val lhs = VertexRelation(Reference("c1"), Relation(Label("Cat")), expr = True)
    val rhs =
      EdgeRelation(
        Reference("e"), Relation(Label("Enemy")), expr = True,
        fromRel = VertexRelation(Reference("c1"), Relation(Label("Cat")), True),
        toRel = VertexRelation(Reference("c2"), Relation(Label("Cat")), True))
    val vertexScan = VertexScan(lhs, DefaultGraph, PlannerContext(db))
    val edgeScan = EdgeScan(rhs, DefaultGraph, PlannerContext(db))
    val join = LeftOuterJoin(lhs, rhs)
    join.children = List(vertexScan, edgeScan)
    val actualDf = sparkPlanner.solveBindingTable(BindingTableOp(join))

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
      actualDf.select(
        s"c1$$$labelCol", s"c1$$$idCol", "c1$name", "c1$age", "c1$weight", "c1$onDiet",
        s"e$$$labelCol", s"e$$$idCol", s"e$$$fromIdCol", s"e$$$toIdCol", "e$since", "e$fights",
        s"c2$$$labelCol", s"c2$$$idCol", "c2$name", "c2$age", "c2$weight", "c2$onDiet"),
      expectedDf.select(
        s"c1$$$labelCol", "c1Id", "c1Name", "c1Age", "c1Weight", "c1OnDiet",
        "elabel", "eId", s"$fromIdCol", s"$toIdCol", "since", "fights",
        s"c2Label", "c2Id", "c2Name", "c2Age", "c2Weight", "c2OnDiet"))
  }

  test("Binding table of Select - (c:Cat) WHERE c.weight > 4 AND c.onDiet = True") {
    val expr = And(
      Gt(PropertyRef(Reference("c"), PropertyKey("weight")), IntLiteral(4)),
      Eq(PropertyRef(Reference("c"), PropertyKey("onDiet")), True))
    val relation = VertexRelation(Reference("c"), Relation(Label("Cat")), expr)
    val scan = VertexScan(relation, DefaultGraph, PlannerContext(db))
    val select = Select(relation, expr)
    select.children = List(scan, expr)
    val actualDf = sparkPlanner.solveBindingTable(BindingTableOp(select))

    val expectedHeader: Seq[String] =
      Seq(s"c$$$labelCol", s"c$$$idCol", "c$name", "c$age", "c$weight", "c$onDiet")
    compareHeaders(expectedHeader, actualDf)

    val expectedDf =
      Seq(hosico, maru).toDF.withColumn(tableLabelColumn.columnName, lit("Cat"))
    compareDfs(
      actualDf.select(s"c$$$labelCol", s"c$$$idCol", "c$name", "c$age", "c$weight", "c$onDiet"),
      expectedDf.select(labelCol, idCol, "name", "age", "weight", "onDiet"))
  }


  /**************************** Helpers to create test expectations. ******************************/
  private def createCatEatsFoodTable(tuples: Seq[(Cat, Eats, Food)],
                                     fromRef: String, edgeRef: String, toRef: String): DataFrame = {
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
