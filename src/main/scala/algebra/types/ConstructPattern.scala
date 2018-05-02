package algebra.types

import algebra.expressions._
import algebra.operators.{RemoveClause, SetClause}

/**
  * A graph pattern used in building a new graph from the binding table of a match sub-clause.
  *
  * In G-CORE grammar, a graph pattern is expressed through a succession of vertices and connections
  * between them, such as edges or paths. For example, the following is a valid construct pattern:
  *
  * (v0) -[e0]-> (v1) -[e1]-> (v2) ... -[en-2]-> (vn-1)
  *
  * In the algebraic tree, we express this graph topology as a sequence of [[ConnectionConstruct]]s.
  * The above pattern then becomes:
  *
  * [e0, e1, e2, ... en-2]
  *
  * , where each ei will be an [[EdgeConstruct]]. Similarly, a [[ConnectionConstruct]] through a
  * stored path will become a [[StoredPathConstruct]] and through a virtual path a
  * [[VirtualPathConstruct]]. These are all [[DoubleEndpointConstruct]]s and have a right and a left
  * endpoint determined by the variables used in the [[ConnectionConstruct]] as read from left to
  * right. If a single vertex is specified in the pattern, then we use a [[VertexConstruct]] node in
  * the tree, which is a [[SingleEndpointConn]]ection.
  *
  * In G-CORE, variables are mutable. If we want to modify the structure of a matched variable we
  * can use [[SetClause]]s or [[RemoveClause]]s on its labels and properties. Note that this does
  * not change the on-disk representation, rather only the result of that particular query.
  *
  * Newly constructed variables can be assigned labels and properties through an
  * [[ObjectConstructPattern]].
  *
  * Graph aggregation is achieved through a [[GroupDeclaration]], which specifies which grouping
  * sets (such as a property's values, for example) to use in the aggregation. If no
  * [[GroupDeclaration]] is specified, then no other grouping is performed, other than the default
  * one.
  *
  * If it is intended to modify the identity of an entity, a copy pattern can be used to create a
  * copy of that entity. This is expressed through a [[Reference]], which will contain the name of
  * the copy. The entity's labels and properties are passed on to the copy, but the copy can be
  * changed in ways that would break the original's identity (such as endpoints for edges or
  * assigning vertex-specific labels to edges, etc.).
  */
case class ConstructPattern(topology: Seq[ConnectionConstruct]) extends AlgebraType {
  children = topology
}

case class GroupDeclaration(groupingSets: Seq[AlgebraExpression]) extends AlgebraType {
  children = groupingSets
}

abstract class ConnectionConstruct(ref: Reference,
                                   copyPattern: Option[Reference],
                                   groupDeclaration: Option[GroupDeclaration],
                                   expr: ObjectConstructPattern) extends AlgebraType {

  def getRef: Reference = ref
  def getExpr: ObjectConstructPattern = expr
  def getGroupDeclaration: Option[GroupDeclaration] = groupDeclaration
}

abstract class SingleEndpointConstruct(ref: Reference,
                                       copyPattern: Option[Reference],
                                       groupDeclaration: Option[GroupDeclaration],
                                       expr: ObjectConstructPattern)
  extends ConnectionConstruct(ref, copyPattern, groupDeclaration, expr) {

  children = List(ref, expr) ++ copyPattern.toList ++ groupDeclaration.toList
}

abstract class DoubleEndpointConstruct(connName: Reference,
                                       connType: ConnectionType,
                                       leftEndpoint: SingleEndpointConstruct,
                                       rightEndpoint: SingleEndpointConstruct,
                                       copyPattern: Option[Reference],
                                       groupDeclaration: Option[GroupDeclaration],
                                       expr: ObjectConstructPattern)
  extends ConnectionConstruct(connName, copyPattern, groupDeclaration, expr) {

  children =
    List(connName, leftEndpoint, rightEndpoint, expr) ++
      copyPattern.toList ++ groupDeclaration.toList

  def getLeftEndpoint: SingleEndpointConstruct = leftEndpoint
  def getRightEndpoint: SingleEndpointConstruct = rightEndpoint
  def getConnType: ConnectionType = connType
}

case class VertexConstruct(ref: Reference,
                           copyPattern: Option[Reference],
                           groupDeclaration: Option[GroupDeclaration],
                           expr: ObjectConstructPattern)
  extends SingleEndpointConstruct(ref, copyPattern, groupDeclaration, expr)

case class EdgeConstruct(connName: Reference,
                         connType: ConnectionType,
                         leftEndpoint: SingleEndpointConstruct,
                         rightEndpoint: SingleEndpointConstruct,
                         copyPattern: Option[Reference],
                         groupDeclaration: Option[GroupDeclaration],
                         expr: ObjectConstructPattern)
  extends DoubleEndpointConstruct(
    connName, connType, leftEndpoint, rightEndpoint, copyPattern, groupDeclaration, expr) {
}

case class StoredPathConstruct(connName: Reference,
                               connType: ConnectionType,
                               leftEndpoint: SingleEndpointConstruct,
                               rightEndpoint: SingleEndpointConstruct,
                               copyPattern: Option[Reference],
                               expr: ObjectConstructPattern)
  extends DoubleEndpointConstruct(
    connName, connType, leftEndpoint, rightEndpoint, copyPattern, groupDeclaration = None, expr)

case class VirtualPathConstruct(connName: Reference,
                                connType: ConnectionType,
                                leftEndpoint: SingleEndpointConstruct,
                                rightEndpoint: SingleEndpointConstruct)
  extends DoubleEndpointConstruct(
    connName, connType, leftEndpoint, rightEndpoint,
    copyPattern = None, groupDeclaration = None,
    expr = ObjectConstructPattern(LabelAssignments(Seq.empty), PropAssignments(Seq.empty)))
