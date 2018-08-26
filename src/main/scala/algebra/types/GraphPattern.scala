package algebra.types

import algebra.expressions._
import algebra.trees._
import common.compiler.Context
import common.exceptions.UnsupportedOperation
import schema.EntitySchema

/**
  * A graph pattern to match in a [[Graph]].
  *
  * In G-CORE grammar, a graph pattern is expressed through a succession of vertices and connections
  * between them, such as edges or paths. For example, the following is a valid match pattern:
  *
  * (v0) -[e0]-> (v1) -[e1]-> (v2) ... -[en-2]-> (vn-1)
  *
  * In the algebraic tree, we express this graph topology as a sequence of [[Connection]]s. The
  * above pattern then becomes:
  *
  * [e0, e1, e2, ... en-2]
  *
  * , where each ei will be an [[Edge]]. Similarly, a [[Connection]] through a path will become a
  * [[Path]]. The [[Edge]] and [[Path]] are both [[DoubleEndpointConn]]ections and have a right and
  * a left endpoint determined by the variables used in the [[Connection]] as read from left to
  * right. If a single vertex in the pattern, then we use a [[Vertex]] node in the tree, which is a
  * [[SingleEndpointConn]]ection.
  *
  * Each variable match pattern can specify conditions on labels and properties. We call this the
  * pattern of an object. The pattern is the conjunction of a [[ConjunctLabels]] and a [[WithProps]]
  * expression. Any of them can be empty, in which case they will be substituted with the [[True]]
  * literal. The two predicates are wrapped in an [[ObjectPattern]] node.
  *
  * The [[ConjunctLabels]] predicate represents a conjunction of disjunct lists of labels. We
  * translate each disjunction as a [[DisjunctLabels]] [[AlgebraExpression]] and then rewrite the
  * conjunction of [[DisjunctLabels]]s as follows:
  *
  * [DLS0, DLS1, DLS2, ... DLSn-1] =
  * = And(DLS0, [DLS1, DLS2, ..., DLSn-1) =
  * = And(DLS0, And(DLS1, [DLS2, ..., DLSn-1])) = ...
  *
  * , where DLSi(labels: Seq[Literal]) = DisjunctLabels(Li0, Li1, ...)
  *
  * The [[WithProps]] predicate represents a conjunction of property value unrolling. We translate
  * the predicate to an [[Eq]] expression between the [[PropertyKey]] and the property substitute.
  * The rewrite of the conjunction is:
  *
  * [Prop0, Prop1, Prop2 ... Propn-1] =
  * = And(Prop0, [Prop1, Prop2, ... Propn-1]) =
  * = And(Prop0, And(Prop1, [Prop2, ... Propn-1])) = ...
  *
  * , where Propi(prop: Literal, value: Literal) = Eq(prop, value)
  */
case class GraphPattern(topology: Seq[Connection]) extends AlgebraType {
  children = topology
}

/******************************** Type of path to query for. **************************************/
abstract class PathQuantifier extends AlgebraType
case class Shortest(qty: Integer, isDistinct: Boolean) extends PathQuantifier {

  override def toString: String = s"$name [$qty, isDistinct = $isDistinct]"
}
case object AllPaths extends PathQuantifier
/**************************************************************************************************/

/********************************** Path expression. **********************************************/
abstract class PathExpression extends AlgebraType

case class KleeneStar(labels: DisjunctLabels, lowerBound: Int, upperBound: Int)
  extends PathExpression with SemanticCheck {

  children = List(labels)

  override def name: String = s"${super.name} [lowerBound = $lowerBound, upperBound = $upperBound]"

  override def check(): Unit = {
    if (lowerBound > 0 || upperBound < Int.MaxValue)
      throw UnsupportedOperation("Kleene bounds are not supported in path expressions.")
  }
}

case class KleeneUnion(lhs: PathExpression, rhs: PathExpression)
  extends PathExpression with SemanticCheck {

  children = List(lhs, rhs)

  override def check(): Unit =
    throw UnsupportedOperation("Path expression union is not supported.")
}

case class KleeneConcatenation(lhs: PathExpression, rhs: PathExpression)
  extends PathExpression with SemanticCheck {

  children = List(lhs, rhs)

  override def check(): Unit =
    throw UnsupportedOperation("Path expression concatenation is not supported.")
}

case class MacroNameReference(reference: Reference) extends PathExpression {
  children = List(reference)
}
/**************************************************************************************************/

/******************************* Abstract connections in graph. ***********************************/
abstract class Connection(ref: Reference, expr: ObjectPattern) extends AlgebraType
  with SemanticCheckWithContext {

  def getRef: Reference = ref
  def getExpr: AlgebraExpression = expr

  def schemaOfEntityType(context: GraphPatternContext): EntitySchema

  override def checkWithContext(context: Context): Unit = {
    val schema = schemaOfEntityType(context.asInstanceOf[GraphPatternContext])
    expr.forEachUp {
      case ObjectPattern(True, propsPred: WithProps) =>
        propsPred
          .checkWithContext(
            PropertyContext(
              graphName = context.asInstanceOf[GraphPatternContext].graphName,
              labelsExpr = None,
              schema = schema))

      case ObjectPattern(labelsPred: ConjunctLabels, propsPred: WithProps) =>
        propsPred
          .checkWithContext(
            PropertyContext(
              graphName = context.asInstanceOf[GraphPatternContext].graphName,
              labelsExpr = Some(labelsPred),
              schema = schema))

      case dl: DisjunctLabels =>
        dl.checkWithContext(
          DisjunctLabelsContext(
            graphName = context.asInstanceOf[GraphPatternContext].graphName,
            schema = schema))

      case _ =>
    }
  }
}

abstract class SingleEndpointConn(ref: Reference, expr: ObjectPattern)
  extends Connection(ref, expr)
abstract class DoubleEndpointConn(connName: Reference,
                                  connType: ConnectionType,
                                  leftEndpoint: SingleEndpointConn,
                                  rightEndpoint: SingleEndpointConn,
                                  expr: ObjectPattern)
  extends Connection(connName, expr) with SemanticCheck {

  def getLeftEndpoint: SingleEndpointConn = leftEndpoint
  def getRightEndpoint: SingleEndpointConn = rightEndpoint

  override def checkWithContext(context: Context): Unit = {
    super.checkWithContext(context)
    rightEndpoint.checkWithContext(context)
    leftEndpoint.checkWithContext(context)
  }

  /** Undirected or bi-directed connections are not supported in current version of interpreter. */
  override def check(): Unit = {
    if (connType == UndirectedConn || connType == InOutConn)
      throw UnsupportedOperation(s"Connection type $connType unsupported.")
  }
}
/**************************************************************************************************/

/********************************* Concrete connections in graph **********************************/
case class Vertex(vertexRef: Reference, expr: ObjectPattern)
  extends SingleEndpointConn(vertexRef, expr) {

  children = List(vertexRef, expr)

  override def schemaOfEntityType(context: GraphPatternContext): EntitySchema =
    context.schema.vertexSchema
}

case class Edge(connName: Reference,
                leftEndpoint: SingleEndpointConn,
                rightEndpoint: SingleEndpointConn,
                connType: ConnectionType,
                expr: ObjectPattern)
  extends DoubleEndpointConn(connName, connType, leftEndpoint, rightEndpoint, expr) {

  children = List(connName, leftEndpoint, rightEndpoint, connType, expr)

  override def schemaOfEntityType(context: GraphPatternContext): EntitySchema =
    context.schema.edgeSchema
}

case class Path(connName: Reference,
                // If binding is not provided, we should not truly bind the result - an unnamed path
                // is actually a reachability test.
                isReachableTest: Boolean,
                leftEndpoint: SingleEndpointConn,
                rightEndpoint: SingleEndpointConn,
                connType: ConnectionType,
                expr: ObjectPattern,
                quantifier: PathQuantifier,
                // If COST is not mentioned, we are not interested in computing it by default.
                costVarDef: Option[Reference],
                isObj: Boolean,
                pathExpression: Option[PathExpression])
  extends DoubleEndpointConn(connName, connType, leftEndpoint, rightEndpoint, expr) {

  children =
    List(connName, leftEndpoint, rightEndpoint, connType, expr, quantifier) ++ costVarDef.toList ++
      pathExpression.toList

  override def toString: String = s"$name [isObjectified = $isObj]"

  override def schemaOfEntityType(context: GraphPatternContext): EntitySchema =
    context.schema.pathSchema

  override def check(): Unit = {
    super.check()

    val acceptedQuantifierConfig: Boolean =
      (isObj && quantifier == AllPaths) ||
        (!isObj && quantifier == Shortest(qty = 1, isDistinct = false) && pathExpression.isDefined)

    if (!acceptedQuantifierConfig)
      throw UnsupportedOperation(s"Unsupported path configuration: " +
        s"${if (isObj) "objectified" else "virtual"} path, " +
        s"quantifier = $quantifier, path expression = $pathExpression")
  }
}
