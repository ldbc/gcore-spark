package ir.algebra.types

import ir.algebra.expressions._

/**
  * A graph pattern to match against.
  *
  * A graph pattern is expressed through a succession of vertices and connections between them,
  * such as edges or paths. For example, the following is a valid match pattern:
  *
  * (v0) -[e0]-> (v1) -[e1]-> (v2) ... -[en-2]-> (vn-1)
  *
  * The topology of the connection is expressed as a sequence of connections. The above pattern
  * will become:
  *
  * [e0, e1, e2, ... en-2]
  *
  * , where each ei will be the [[AlgebraType]] [[Edge]]. Similarly, a connection through a path
  * will be the [[AlgebraType]] [[Path]]. These are both [[DoubleEndpointConn]]ections. If a
  * single vertex is specified in the pattern, then we use the [[AlgebraType]] [[Vertex]], which
  * is a [[SingleEndpointConn]]ection.
  *
  * Each entity match pattern can specify conditions on labels and properties. We call this the
  * pattern of an object. The pattern is the conjunction of a [[WithLabels]] and a [[WithProps]]
  * expression. Any of them can be empty, in which case they will be substituted with the [[True]]
  * expression.
  *
  * The [[WithLabels]] predicate represents a conjunction of disjunct lists of labels. We translate
  * each disjunction as a [[HasLabel]] [[AlgebraExpression]] and then rewrite the conjunction of
  * [[HasLabel]]s (which is actually a sequence of disjunctions) as follows:
  *
  * [DLS0, DLS1, DLS2, ... DLSn-1] =
  * = And(DLS0, [DLS1, DLS2, ..., DLSn-1) =
  * = And(DLS0, And(DLS1, [DLS2, ..., DLSn-1])) = ...
  *
  * , where DLSi(labels: Seq[Literal]) = HasLabel(labels)
  *
  * The [[WithProps]] predicate represents a conjunction of property value equality conditions. We
  * translate the condition to an [[Eq]] [[AlgebraExpression]] between the property key (a
  * [[Literal]]) and the property value (a [[Literal]]). The rewrite of the conjunection (which is
  * actually a list of property conditions) goes as follows:
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

abstract class ConnectionType extends AlgebraType
case class InConn() extends ConnectionType
case class OutConn() extends ConnectionType
case class InOutConn() extends ConnectionType
case class UndirectedConn() extends ConnectionType

abstract class Connection extends AlgebraType
abstract class SingleEndpointConn extends Connection
abstract class DoubleEndpointConn extends Connection {
  def getLeftEndpoint: SingleEndpointConn
  def getRightEndpoint: SingleEndpointConn
}

abstract class PathQuantifier extends AlgebraType
case class Shortest(qty: Integer, isDistinct: Boolean) extends PathQuantifier {

  override def toString: String = s"$name [$qty, isDistinct = $isDistinct]"
}
case class AllPaths() extends PathQuantifier

case class Vertex(vertexRef: Reference, expr: AlgebraExpression) extends SingleEndpointConn {

  children = List(vertexRef, expr)
}

case class Edge(connName: Reference,
                leftEndpoint: SingleEndpointConn,
                rightEndpoint: SingleEndpointConn,
                connType: ConnectionType, // ObjectMatchPattern
                expr: AlgebraExpression) extends DoubleEndpointConn {

  children = List(connName, leftEndpoint, rightEndpoint, connType, expr)

  override def getLeftEndpoint: SingleEndpointConn = leftEndpoint

  override def getRightEndpoint: SingleEndpointConn = rightEndpoint
}

case class Path(connName: Option[Reference], // If binding is not provided, we should not try to
                                             // bind the results to any variable. An unnamed path
                                             // is actually a reachability test.
                leftEndpoint: SingleEndpointConn,
                rightEndpoint: SingleEndpointConn,
                connType: ConnectionType,
                expr: AlgebraExpression, // ObjectMatchPattern
                quantifier: Option[PathQuantifier],
                costVarDef: Option[Literal[String]], // If COST is not mentioned, we are not
                                                     // interested in computing it by default.
                isObj: Boolean) extends DoubleEndpointConn {
               // TODO: path expression

  children = connName.toList ++
    List(leftEndpoint, rightEndpoint, connType, expr) ++
    quantifier.toList ++
    costVarDef.toList
  override def toString: String = s"$name [isObjectified = $isObj]"

  override def getLeftEndpoint: SingleEndpointConn = leftEndpoint

  override def getRightEndpoint: SingleEndpointConn = rightEndpoint
}
