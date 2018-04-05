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
  * pattern of an object. The pattern is the conjunction of a [[WithLabels]] and a [[WithProps]]
  * expression. Any of them can be empty, in which case they will be substituted with the [[True]]
  * literal. The two predicates are wrapped in an [[ObjectPattern]] node.
  *
  * The [[WithLabels]] predicate represents a conjunction of disjunct lists of labels. We translate
  * each disjunction as a [[HasLabel]] [[AlgebraExpression]] and then rewrite the conjunction of
  * [[HasLabel]]s as follows:
  *
  * [DLS0, DLS1, DLS2, ... DLSn-1] =
  * = And(DLS0, [DLS1, DLS2, ..., DLSn-1) =
  * = And(DLS0, And(DLS1, [DLS2, ..., DLSn-1])) = ...
  *
  * , where DLSi(labels: Seq[Literal]) = HasLabel(Li0, Li1, ...)
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

/** Where does the edge point? */
abstract class ConnectionType extends AlgebraType
case object InConn extends ConnectionType
case object OutConn extends ConnectionType
case object InOutConn extends ConnectionType
case object UndirectedConn extends ConnectionType

/** Type of path to query for. */
abstract class PathQuantifier extends AlgebraType
case class Shortest(qty: Integer, isDistinct: Boolean) extends PathQuantifier {

  override def toString: String = s"$name [$qty, isDistinct = $isDistinct]"
}
case object AllPaths extends PathQuantifier


/** Abstract connections in graph. */
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

      case ObjectPattern(labelsPred: WithLabels, propsPred: WithProps) =>
        propsPred
          .checkWithContext(
            PropertyContext(
              graphName = context.asInstanceOf[GraphPatternContext].graphName,
              labelsExpr = Some(labelsPred),
              schema = schema))

      case hl: HasLabel =>
        hl.checkWithContext(
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
                quantifier: Option[PathQuantifier],
                // If COST is not mentioned, we are not interested in computing it by default.
                costVarDef: Option[Reference],
                isObj: Boolean)
                // TODO: path expression
  extends DoubleEndpointConn(connName, connType, leftEndpoint, rightEndpoint, expr) {

  children = List(connName, leftEndpoint, rightEndpoint, connType, expr) ++
    quantifier.toList ++ costVarDef.toList

  override def toString: String = s"$name [isObjectified = $isObj]"

  override def schemaOfEntityType(context: GraphPatternContext): EntitySchema =
    context.schema.pathSchema

  /** Virtual paths are not supported in current version of interpreter. */
  override def check(): Unit = {
    super.check()
    if (!isObj)
      throw UnsupportedOperation(s"Virtual paths unsupported.")
  }
}
