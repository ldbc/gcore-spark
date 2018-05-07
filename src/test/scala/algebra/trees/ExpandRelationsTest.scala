package algebra.trees

import algebra.expressions.{Exists, Label, Reference}
import algebra.operators._
import algebra.trees.CustomMatchers._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import parser.SpoofaxParser
import parser.trees.ParseContext

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ExpandRelationsTest extends FunSuite
  with BeforeAndAfterAll with Matchers with TestGraphWrapper {

  private val spoofaxParser: SpoofaxParser = SpoofaxParser(ParseContext(catalog))
  private val expandRelations: ExpandRelations = ExpandRelations(AlgebraContext(catalog))

  override def beforeAll() {
    super.beforeAll()
    catalog.registerGraph(catsGraph)
    catalog.setDefaultGraph("cats graph")
  }

  test("Strict label for vertex is preserved - (v:Cat)") {
    val query = "CONSTRUCT () MATCH (v:Cat)"
    val simpleMatches = extractNonOptSimpleMatches(query)

    simpleMatches.head should matchPattern {
      case SimpleMatchRelation(VertexRelation(Reference("v"), Relation(Label("Cat")), _), _, _) =>
    }
  }

  test("Unlabeled vertex is expanded into all available labels - (v)") {
    val query = "CONSTRUCT () MATCH (v)"
    val simpleMatches = extractNonOptSimpleMatches(query)

    val refs: Seq[Reference] =
      simpleMatches.map(_.children.head.asInstanceOf[VertexRelation].ref)
    val labels: Seq[RelationLike] =
      simpleMatches.map(_.children.head.asInstanceOf[VertexRelation].labelRelation)

    assert(refs.lengthCompare(3) == 0)
    assert(refs.toSet == Set(Reference("v")))
    assert(labels.lengthCompare(3) == 0)
    assert(labels.toSet ==
      Set(Relation(Label("Cat")), Relation(Label("Food")), Relation(Label("Country"))))
  }

  test("Mix labeled and unlabeled vertex - (v:Cat), (w)") {
    val query = "CONSTRUCT () MATCH (v:Cat), (w)"
    val simpleMatches = extractNonOptSimpleMatches(query)

    val refs: Set[Reference] =
      simpleMatches.map(_.children.head.asInstanceOf[VertexRelation].ref).toSet
    assert(refs == Set(Reference("v"), Reference("w")))

    val refToLabels: Map[Reference, Seq[RelationLike]] =
      refs
        .map(ref => {
          val labels: Seq[RelationLike] =
            simpleMatches
              .filter(_.children.head.asInstanceOf[VertexRelation].ref == ref)
              .map(_.children.head.asInstanceOf[VertexRelation].labelRelation)
          ref -> labels
        })
        .toMap

    assert(refToLabels(Reference("v")).lengthCompare(1) == 0)
    assert(refToLabels(Reference("v")).toSet == Set(Relation(Label("Cat"))))
    assert(refToLabels(Reference("w")).lengthCompare(3) == 0)
    assert(refToLabels(Reference("w")).toSet ==
      Set(Relation(Label("Cat")), Relation(Label("Food")), Relation(Label("Country"))))
  }

  test("Strict labels for edge/path and endpoints are preserved - " +
    "(f:Food)-[e:MadeIn]->(c:Country), (c:Cat)-/@p:ToGourmand/->(f:Food)") {
    val edgeQuery = "CONSTRUCT () MATCH (f:Food)-[e:MadeIn]->(c:Country)"
    val edgeSimpleMatches = extractNonOptSimpleMatches(edgeQuery)

    val pathQuery = "CONSTRUCT () MATCH (c:Cat)-/@p:ToGourmand/->(f:Food)"
    val pathSimpleMatches = extractNonOptSimpleMatches(pathQuery)

    edgeSimpleMatches.head should matchEdgeFoodMadeInCountry
    pathSimpleMatches.head should matchPathCatToGourmandFood
  }

  test("Labeled edge/path determines strict vertex labels - " +
    "(f)-[e:MadeIn]->(c), (c)-/@p:ToGourmand/->(f)") {
    val edgeQuery = "CONSTRUCT () MATCH (f)-[e:MadeIn]->(c)"
    val edgeSimpleMatches = extractNonOptSimpleMatches(edgeQuery)

    val pathQuery = "CONSTRUCT () MATCH (c)-/@p:ToGourmand/->(f)"
    val pathSimpleMatches = extractNonOptSimpleMatches(pathQuery)

    edgeSimpleMatches.head should matchEdgeFoodMadeInCountry
    pathSimpleMatches.head should matchPathCatToGourmandFood
  }

  test("Labeled vertices determine strict edge/path label - " +
    "(f:Food)-[e]->(c:Country), (c)-/@p:ToGourmand/->(f)") {
    val edgeQuery = "CONSTRUCT () MATCH (f:Food)-[e]->(c:Country)"
    val edgeSimpleMatches = extractNonOptSimpleMatches(edgeQuery)

    val pathQuery = "CONSTRUCT () MATCH (c:Cat)-/@p/->(f:Food)"
    val pathSimpleMatches = extractNonOptSimpleMatches(pathQuery)

    edgeSimpleMatches.head should matchEdgeFoodMadeInCountry
    pathSimpleMatches.head should matchPathCatToGourmandFood
  }

  test("Labeled vertex determine strict vertex and edge/path label - " +
    "(f:Food)-[e]->(c), (f)-[e]->(c:Country), (c:Cat)-/@p/->(f), (c)-/@p/->(f:Food)") {
    val edgeFoodQuery = "CONSTRUCT () MATCH (f:Food)-[e]->(c)"
    val simpleMatchesEdgeFoodQuery = extractNonOptSimpleMatches(edgeFoodQuery)

    val edgeCountryQuery = "CONSTRUCT () MATCH (f)-[e]->(c:Country)"
    val simpleMatchesEdgeCountryQuery = extractNonOptSimpleMatches(edgeCountryQuery)

    val pathCatQuery = "CONSTRUCT () MATCH (c:Cat)-/@p/->(f)"
    val simpleMatchesPathCatQuery = extractNonOptSimpleMatches(pathCatQuery)

    val pathFoodQuery = "CONSTRUCT () MATCH (c)-/@p/->(f:Food)"
    val simpleMatchesPathFoodQuery = extractNonOptSimpleMatches(pathFoodQuery)

    simpleMatchesEdgeCountryQuery.head should matchEdgeFoodMadeInCountry
    simpleMatchesEdgeFoodQuery.head should matchEdgeFoodMadeInCountry
    simpleMatchesPathCatQuery.head should matchPathCatToGourmandFood
    simpleMatchesPathFoodQuery.head should matchPathCatToGourmandFood
  }

  test("Labeled vertices determine multi-valued edge label - (c1:Cat)-[e]->(c2:Cat)") {
    val query = "CONSTRUCT () MATCH (c1:Cat)-[e]->(c2:Cat)"
    val simpleMatches = extractNonOptSimpleMatches(query)

    val labelTuples: Seq[(RelationLike, RelationLike, RelationLike)] =
      simpleMatches
        .map(m => {
          val er: EdgeRelation = m.children.head.asInstanceOf[EdgeRelation]
          (er.fromRel.labelRelation, er.labelRelation, er.toRel.labelRelation)
        })

    assert(labelTuples.lengthCompare(2) == 0)
    assert(labelTuples.toSet ==
      Set(
        (Relation(Label("Cat")), Relation(Label("Enemy")), Relation(Label("Cat"))),
        (Relation(Label("Cat")), Relation(Label("Friend")), Relation(Label("Cat"))))
    )
  }

  test("Labeled vertex determines multi-valued vertex and edge labels - (c:Cat)-[e]->(v)") {
    val query = "CONSTRUCT () MATCH (c:Cat)-[e]->(v)"
    val simpleMatches = extractNonOptSimpleMatches(query)

    val labelTuples: Seq[(RelationLike, RelationLike, RelationLike)] =
      simpleMatches
        .map(m => {
          val er: EdgeRelation = m.children.head.asInstanceOf[EdgeRelation]
          (er.fromRel.labelRelation, er.labelRelation, er.toRel.labelRelation)
        })

    assert(labelTuples.lengthCompare(3) == 0)
    assert(labelTuples.toSet ==
      Set(
        (Relation(Label("Cat")), Relation(Label("Eats")), Relation(Label("Food"))),
        (Relation(Label("Cat")), Relation(Label("Enemy")), Relation(Label("Cat"))),
        (Relation(Label("Cat")), Relation(Label("Friend")), Relation(Label("Cat"))))
    )
  }

  test("Loose edge/path pattern determines all combinations of vertex and edge labels - " +
    "(v)-[e]->(w), (v)-/@p/->(w)") {
    val edgeQuery = "CONSTRUCT () MATCH (v)-[e]->(w)"
    val edgeSimpleMatches = extractNonOptSimpleMatches(edgeQuery)

    val pathQuery = "CONSTRUCT () MATCH (c)-/@p/->(f)"
    val pathSimpleMatches = extractNonOptSimpleMatches(pathQuery)

    val edgeLabelTuples: Seq[(RelationLike, RelationLike, RelationLike)] =
      edgeSimpleMatches
        .map(m => {
          val er: EdgeRelation = m.children.head.asInstanceOf[EdgeRelation]
          (er.fromRel.labelRelation, er.labelRelation, er.toRel.labelRelation)
        })

    val pathLabelTuples: Seq[(RelationLike, RelationLike, RelationLike)] =
      pathSimpleMatches
        .map(m => {
          val pr: StoredPathRelation = m.children.head.asInstanceOf[StoredPathRelation]
          (pr.fromRel.labelRelation, pr.labelRelation, pr.toRel.labelRelation)
        })

    assert(edgeLabelTuples.lengthCompare(4) == 0)
    assert(edgeLabelTuples.toSet ==
      Set(
        (Relation(Label("Food")), Relation(Label("MadeIn")), Relation(Label("Country"))),
        (Relation(Label("Cat")), Relation(Label("Eats")), Relation(Label("Food"))),
        (Relation(Label("Cat")), Relation(Label("Enemy")), Relation(Label("Cat"))),
        (Relation(Label("Cat")), Relation(Label("Friend")), Relation(Label("Cat"))))
    )

    assert(pathLabelTuples.lengthCompare(1) == 0)
    assert(pathLabelTuples.toSet ==
      Set((Relation(Label("Cat")), Relation(Label("ToGourmand")), Relation(Label("Food"))))
    )
  }

  test("Chained patterns become sequence of SimpleMatchRelations") {
    val query = "CONSTRUCT () MATCH (c1:Cat)-[e1]->(f:Food)-[e2]->(c2:Country)"
    val relations = extractNonOptSimpleMatches(query)

    assert(relations.lengthCompare(2) == 0)

    val refTuples: Set[(Reference, Reference, Reference)] =
      relations
        .map(rel => {
          val er: EdgeRelation = rel.children.head.asInstanceOf[EdgeRelation]
          (er.fromRel.ref, er.ref, er.toRel.ref)
        })
        .toSet

    assert(refTuples ==
      Set(
        (Reference("c1"), Reference("e1"), Reference("f")),
        (Reference("f"), Reference("e2"), Reference("c2"))
      ))
  }

  test("Chained and comma-separated patterns become sequence of SimpleMatchRelations") {
    val query =
      "CONSTRUCT () MATCH (c1)-[e1:Enemy]->(c2), (c3:Cat)-[e2]->(f:Food)-[e3]->(c4:Country)"
    val relations = extractNonOptSimpleMatches(query)

    assert(relations.lengthCompare(3) == 0)

    val refTuples: Set[(Reference, Reference, Reference)] =
      relations
        .map(rel => {
          val er: EdgeRelation = rel.children.head.asInstanceOf[EdgeRelation]
          (er.fromRel.ref, er.ref, er.toRel.ref)
        })
        .toSet

    assert(refTuples ==
      Set(
        (Reference("c1"), Reference("e1"), Reference("c2")),
        (Reference("c3"), Reference("e2"), Reference("f")),
        (Reference("f"), Reference("e3"), Reference("c4")))
    )
  }

  test("Solve chained pattern: (c1:Cat)->(f:Food)->(c2)") {
    val query = "CONSTRUCT () MATCH (c1:Cat)-[e1]->(f:Food)-[e2]->(c2)"
    val expected: Map[Reference, Seq[(Label, Label, Label)]] =
      Map(
        Reference("e1") -> Seq((Label("Cat"), Label("Eats"), Label("Food"))),
        Reference("e2") -> Seq((Label("Food"), Label("MadeIn"), Label("Country")))
      )
    testChainedPattern(query, expectedEdgeOrPath = expected, expectedVertex = Map.empty)
  }

  test("Solve chained pattern: (c1)->(c2:Cat), (f:Food), (c1)->(f), (f)->(c3)") {
    val query = "CONSTRUCT () MATCH (c1)-[e1]->(c2:Cat), (c1)-[e2]->(f:Food), (f)-[e3]->(c3)"
    val expected: Map[Reference, Seq[(Label, Label, Label)]] =
      Map(
        Reference("e1") ->
          Seq(
            (Label("Cat"), Label("Enemy"), Label("Cat")),
            (Label("Cat"), Label("Friend"), Label("Cat"))
          ),
        Reference("e2") ->
          Seq((Label("Cat"), Label("Eats"), Label("Food"))),
        Reference("e3") ->
          Seq((Label("Food"), Label("MadeIn"), Label("Country")))
      )
    testChainedPattern(query, expectedEdgeOrPath = expected, expectedVertex = Map.empty)
  }

  test("Solve chained pattern: (c1)->(f), (c1:Cat)->(c2)->(f:Food), (f)->(c3)") {
    val query =
      "CONSTRUCT () MATCH (c1)-[e1]->(f), (c1:Cat)-[e2]->(c2)-[e3]->(f:Food), (f)-[e4]->(c3)"
    val expected: Map[Reference, Seq[(Label, Label, Label)]] =
      Map(
        Reference("e1") ->
          Seq((Label("Cat"), Label("Eats"), Label("Food"))),
        Reference("e2") ->
          Seq(
            (Label("Cat"), Label("Enemy"), Label("Cat")),
            (Label("Cat"), Label("Friend"), Label("Cat"))
          ),
        Reference("e3") ->
          Seq((Label("Cat"), Label("Eats"), Label("Food"))),
        Reference("e4") ->
          Seq((Label("Food"), Label("MadeIn"), Label("Country")))
      )
    testChainedPattern(query, expectedEdgeOrPath = expected, expectedVertex = Map.empty)
  }

  test("Solve chained pattern: (c)-/@p/->(f)") {
    val query = "CONSTRUCT () MATCH (c)-/@p/->(f)"
    val expected: Map[Reference, Seq[(Label, Label, Label)]] =
      Map(
        Reference("p") ->
          Seq((Label("Cat"), Label("ToGourmand"), Label("Food")))
      )
    testChainedPattern(query, expectedEdgeOrPath = expected, expectedVertex = Map.empty)
  }

  test("Solve chained pattern: (c1)-/@p/->(f)->(c2), (c2), (c3), (c3)->(v)") {
    val query = "CONSTRUCT () MATCH (c1)-/@p/->(f)-[e1]->(c2), (c2), (c3), (c3)-[e2]->(v)"
    val expectedEdgeOrPath: Map[Reference, Seq[(Label, Label, Label)]] =
      Map(
        Reference("p") ->
          Seq((Label("Cat"), Label("ToGourmand"), Label("Food"))),
        Reference("e1") ->
          Seq((Label("Food"), Label("MadeIn"), Label("Country"))),
        Reference("e2") ->
          Seq(
            (Label("Cat"), Label("Enemy"), Label("Cat")),
            (Label("Cat"), Label("Friend"), Label("Cat")),
            (Label("Cat"), Label("Eats"), Label("Food")),
            (Label("Food"), Label("MadeIn"), Label("Country")))
      )
    val expectedVertex: Map[Reference, Seq[Label]] =
      Map(
        Reference("c2") -> Seq(Label("Country")),
        Reference("c3") -> Seq(Label("Cat"), Label("Food"))
      )
    testChainedPattern(query, expectedEdgeOrPath, expectedVertex)
  }

  test("Optional clauses participate in label inference: (f:Food), OPTIONAL (f)->(c)") {
    val query = "CONSTRUCT () MATCH (f:Food) OPTIONAL (f)-[e]->(c)"
    val expectedEdgeOrPath: Map[Reference, Seq[(Label, Label, Label)]] =
      Map(
        Reference("e") ->
          Seq((Label("Food"), Label("MadeIn"), Label("Country"))))
    val expectedVertex: Map[Reference, Seq[Label]] = Map(Reference("f") -> Seq(Label("Food")))
    testChainedPattern(query, expectedEdgeOrPath, expectedVertex)
  }

  test("Solve chained pattern: (c1)->(f), (f)->(c2:Country) OPTIONAL (c1)-[:Enemy]->(c3)") {
    val query =
      "CONSTRUCT () MATCH (c1)-[e1]->(f), (f)-[e2]->(c2:Country) OPTIONAL (c1)-[e3:Enemy]->(c3)"
    val expectedEdgeOrPath: Map[Reference, Seq[(Label, Label, Label)]] =
      Map(
        Reference("e1") ->
          Seq((Label("Cat"), Label("Eats"), Label("Food"))),
        Reference("e2") ->
          Seq((Label("Food"), Label("MadeIn"), Label("Country"))),
        Reference("e3") ->
          Seq((Label("Cat"), Label("Enemy"), Label("Cat")))
      )
    testChainedPattern(query, expectedEdgeOrPath, expectedVertex = Map.empty)
  }

  test("Solve chained patterns: (c:Cat)->(f:Food) OPTIONAL (c)->(c2:Cat)") {
    val query =
      "CONSTRUCT () MATCH (c:Cat)-[e1]->(f:Food) OPTIONAL (c)-[e2]->(c2:Cat)"
    val expectedEdgeOrPath: Map[Reference, Seq[(Label, Label, Label)]] =
      Map(
        Reference("e1") ->
          Seq((Label("Cat"), Label("Eats"), Label("Food"))),
        Reference("e2") ->
          Seq(
            (Label("Cat"), Label("Friend"), Label("Cat")),
            (Label("Cat"), Label("Enemy"), Label("Cat")))
      )
    testChainedPattern(query, expectedEdgeOrPath, expectedVertex = Map.empty)
  }

  test("Exists clauses participate in label inference: (c) WHERE ()-[:Enemy]->(c)") {
    val query = "CONSTRUCT () MATCH (c) WHERE (v)-[e:Enemy]->(c)"
    val expectedEdgeOrPath: Map[Reference, Seq[(Label, Label, Label)]] =
      Map(Reference("e") -> Seq((Label("Cat"), Label("Enemy"), Label("Cat"))))
    val expectedVertex: Map[Reference, Seq[Label]] = Map(Reference("c") -> Seq(Label("Cat")))
    testChainedPattern(query, expectedEdgeOrPath, expectedVertex)
  }

  test("Exists clauses participate in label inference for optional matches: " +
    "(c) OPTIONAL (c)->(f) WHERE (f:Food)") {
    val query = "CONSTRUCT () MATCH (c) OPTIONAL (c)-[e]->(f) WHERE (f:Food)"
    val expectedEdgeOrPath: Map[Reference, Seq[(Label, Label, Label)]] =
      Map(Reference("e") -> Seq((Label("Cat"), Label("Eats"), Label("Food"))))
    val expectedVertex: Map[Reference, Seq[Label]] =
      Map(Reference("c") -> Seq(Label("Cat")), Reference("f") -> Seq(Label("Food")))
    testChainedPattern(query, expectedEdgeOrPath, expectedVertex)
  }

  /**
    * Extracts all [[SimpleMatchRelation]]s (from the non-optional and optional clauses) after the
    * query has been parsed and rewritten using the [[PatternsToRelations]] and [[ExpandRelations]]
    * rewriters.
    */
  private def extractSimpleMatches(query: String): Seq[AlgebraTreeNode] = {
    extractSimpleMatches(rewrite(query))
  }

  /**
    * Extracts the array of non-optional [[SimpleMatchRelation]]s after the query has been parsed
    * and rewritten using the [[PatternsToRelations]] and [[ExpandRelations]] rewriters.
    */
  private def extractNonOptSimpleMatches(query: String): Seq[AlgebraTreeNode] = {
    extractNonOptSimpleMatches(rewrite(query))
  }

  private def extractNonOptSimpleMatches(queryTree: AlgebraTreeNode): Seq[AlgebraTreeNode] = {
    val matchClause = queryTree.asInstanceOf[Query].getMatchClause
    val nonOptCondMatch = matchClause.children.head
    nonOptCondMatch.children.init
  }

  private def extractSimpleMatches(queryTree: AlgebraTreeNode): Seq[AlgebraTreeNode] = {
    val matchClause = queryTree.asInstanceOf[Query].getMatchClause
    val nonOptCondMatch = matchClause.children.head
    val optCondMatches = matchClause.children.tail

    val existsSimpleMatches: mutable.ArrayBuffer[AlgebraTreeNode] =
      new mutable.ArrayBuffer[AlgebraTreeNode]()
    matchClause forEachDown {
      case e: Exists => existsSimpleMatches ++= e.children
      case _ =>
    }

    nonOptCondMatch.children.init ++ optCondMatches.flatMap(_.children.init) ++ existsSimpleMatches
  }

  private def rewrite(query: String): AlgebraTreeNode = {
    val context = AlgebraContext(catalog, Some(Map.empty)) // all vars in the default graph
    val treeWithExists: AlgebraTreeNode =
      AddGraphToExistentialPatterns(context).rewriteTree(spoofaxParser.parse(query))
    val treeWithRelations = PatternsToRelations rewriteTree treeWithExists
    expandRelations rewriteTree treeWithRelations
  }

  /**
    * Runs the query through the parsing and rewriting pipeline, then checks whether the relations
    * in the resulting array of [[SimpleMatchRelation]]s are labeled as expected. For an
    * [[EdgeRelation]] or a [[StoredPathRelation]] the expected labels are provided as tuples of
    * (from label, edge/path label, to label).
    */
  private
  def testChainedPattern(query: String,
                         expectedEdgeOrPath: Map[Reference, Seq[(Label, Label, Label)]],
                         expectedVertex: Map[Reference, Seq[Label]]): Unit = {
    val relations = extractSimpleMatches(query)
    val edgeOrPathRefs: mutable.ArrayBuffer[Reference] = new ArrayBuffer[Reference]()
    val vertexRefs: mutable.ArrayBuffer[Reference] = new ArrayBuffer[Reference]()
    relations.foreach(simpleMatch => {
      val rel = simpleMatch.children.head
      rel match {
        case er: EdgeRelation => edgeOrPathRefs += er.ref
        case pr: StoredPathRelation => edgeOrPathRefs += pr.ref
        case vr: VertexRelation => vertexRefs += vr.ref
      }
    })
    val actualEdgeOrPath: Map[Reference, Seq[(Label, Label, Label)]] =
      edgeOrPathRefs
        .map(edgeOrPathRef => {
          val labelTuples: Seq[(Label, Label, Label)] =
            relations
              .filter(simpleMatch => {
                val rel = simpleMatch.children.head
                rel match {
                  case er: EdgeRelation => er.ref == edgeOrPathRef
                  case pr: StoredPathRelation => pr.ref == edgeOrPathRef
                  case _ => false
                }
              })
              .map(simpleMatch => {
                val rel = simpleMatch.children.head
                rel match {
                  case er: EdgeRelation =>
                    (er.fromRel.labelRelation.asInstanceOf[Relation].label,
                      er.labelRelation.asInstanceOf[Relation].label,
                      er.toRel.labelRelation.asInstanceOf[Relation].label)
                  case pr: StoredPathRelation =>
                    (pr.fromRel.labelRelation.asInstanceOf[Relation].label,
                      pr.labelRelation.asInstanceOf[Relation].label,
                      pr.toRel.labelRelation.asInstanceOf[Relation].label)
                }
              })
          edgeOrPathRef -> labelTuples
        })
        .toMap
    val actualVertex: Map[Reference, Seq[Label]] =
      vertexRefs
        .map(vertexRef => {
          val vertexLabels: Seq[Label] =
            relations
              .filter(simpleMatch => {
                val rel = simpleMatch.children.head
                rel match {
                  case vr: VertexRelation => vr.ref == vertexRef
                  case _ => false
                }
              })
              .map(simpleMatch => {
                val rel = simpleMatch.children.head
                rel match {
                  case vr: VertexRelation => vr.labelRelation.asInstanceOf[Relation].label
                }
              })
          vertexRef -> vertexLabels
        })
        .toMap

    assert(actualEdgeOrPath.keys.size == expectedEdgeOrPath.keys.size)
    assert(actualVertex.keys.size == expectedVertex.keys.size)

    if (expectedEdgeOrPath.nonEmpty)
      actualEdgeOrPath.keys.foreach(ref => {
        assert(actualEdgeOrPath(ref).lengthCompare(expectedEdgeOrPath(ref).size) == 0)
        assert(actualEdgeOrPath(ref).toSet == expectedEdgeOrPath(ref).toSet)
      })

    if (expectedVertex.nonEmpty)
      actualVertex.keys.foreach(ref => {
        assert(actualVertex(ref).lengthCompare(expectedVertex(ref).size) == 0)
        assert(actualVertex(ref).toSet == expectedVertex(ref).toSet)
      })
  }
}
