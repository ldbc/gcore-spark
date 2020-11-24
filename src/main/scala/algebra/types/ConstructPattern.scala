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

package algebra.types

import algebra.exceptions.AmbiguousMerge
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

  /**
    * Creates a new [[GroupDeclaration]] from the set union of this object's and other's
    * [[groupingSets]].
    */
  def merge(other: GroupDeclaration): GroupDeclaration =
    GroupDeclaration(groupingSets = (this.groupingSets.toSet ++ other.groupingSets.toSet).toSeq)
}

abstract class ConnectionConstruct(ref: Reference,
                                   copyPattern: Option[Reference],
                                   groupDeclaration: Option[GroupDeclaration],
                                   expr: ObjectConstructPattern) extends AlgebraType {

  def getRef: Reference = ref
  def getExpr: ObjectConstructPattern = expr
  def getGroupDeclaration: Option[GroupDeclaration] = groupDeclaration
  def getCopyPattern: Option[Reference] = copyPattern

  /**
    * Creates a new [[ConnectionConstruct]] by mergin together the [[copyPattern]],
    * [[groupDeclaration]] and [[expr]] of this and other object's. The builder is a method that can
    * create a new [[ConnectionConstruct]] from the merged features.
    */
  def merge[T <: ConnectionConstruct](other: T,
                                      builder: (
                                        Reference, Option[Reference], Option[GroupDeclaration],
                                          ObjectConstructPattern) => T): T = {
    if (this.ref != other.getRef)
      throw AmbiguousMerge(
        s"Cannot merge connections of different references: " +
          s"${this.ref.refName} vs ${other.getRef.refName}")

    val mergedCopyPattern: Option[Reference] = {
      if (this.copyPattern.isDefined && other.getCopyPattern.isDefined) {
        val thisCopyRef: Reference = this.copyPattern.get
        val otherCopyRef: Reference = other.getCopyPattern.get
        if (thisCopyRef != otherCopyRef)
          throw AmbiguousMerge(
            s"Ambiguous copy pattern for vertex ${this.ref.refName}: ${thisCopyRef.refName} vs " +
              s"${otherCopyRef.refName}.")
        else
          this.copyPattern
      } else {
        if (this.copyPattern.isDefined)
          this.copyPattern
        else
          other.getCopyPattern
      }
    }

    val mergedGroupDeclaration: Option[GroupDeclaration] = {
      if (this.groupDeclaration.isDefined && other.getGroupDeclaration.isDefined)
        Some(this.groupDeclaration.get merge other.getGroupDeclaration.get)
      else {
        if (this.groupDeclaration.isDefined)
          this.groupDeclaration
        else if (other.getGroupDeclaration.isDefined)
          other.getGroupDeclaration
        else None
      }
    }

    val mergedExprs: ObjectConstructPattern = this.expr merge other.getExpr

    builder(ref, mergedCopyPattern, mergedGroupDeclaration, mergedExprs)
  }
}

abstract class SingleEndpointConstruct(ref: Reference,
                                       copyPattern: Option[Reference],
                                       groupDeclaration: Option[GroupDeclaration],
                                       expr: ObjectConstructPattern)
  extends ConnectionConstruct(ref, copyPattern, groupDeclaration, expr) {

  children = List(ref, expr) ++ copyPattern.toList ++ groupDeclaration.toList

  /**
    * Creates a new [[VertexConstruct]] by merging together the features of this and the other
    * [[SingleEndpointConstruct]].
    */
  def merge(other: SingleEndpointConstruct): SingleEndpointConstruct =
    super.merge[SingleEndpointConstruct](
      other,
      builder = (ref, copyPattern, groupDeclaration, expr) =>
        VertexConstruct(ref, copyPattern, groupDeclaration, expr)
    )
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

  /** Returns the [[Reference]] of the source vertex of this edge. */
  def getFromReference: Reference = connType match {
      case InConn => rightEndpoint.getRef
      case _ => leftEndpoint.getRef
    }

  /** Returns the [[Reference]] of the destination vertex of this edge. */
  def getToReference: Reference = connType match {
      case InConn => leftEndpoint.getRef
      case _ => rightEndpoint.getRef
    }

  /**
    * Creates a new [[EdgeConstruct]] by merging together the features of this and the other
    * [[DoubleEndpointConstruct]].
    */
  def merge(other: DoubleEndpointConstruct): DoubleEndpointConstruct = {
    if (this.getFromReference != other.getFromReference) {
      throw AmbiguousMerge(
        s"Ambiguous source for edge ${this.connName.refName}: " +
          s"${this.getFromReference.refName} vs. ${other.getFromReference.refName}"
      )
    }

    if (this.getToReference != other.getToReference) {
      throw AmbiguousMerge(
        s"Ambiguous destination for edge ${this.connName.refName}: " +
          s"${this.getToReference.refName} vs. ${other.getToReference.refName}"
      )
    }

    if ((this.connType == InOutConn || this.connType == UndirectedConn ||
      other.getConnType == InOutConn || other.getConnType == UndirectedConn) &&
      this.connType != other.getConnType)
      throw AmbiguousMerge(
        s"Ambiguous connection type for edge ${this.connName.refName}: " +
          s"${this.connType} vs. ${other.getConnType}")

    val mergedLeftEndpoint: SingleEndpointConstruct = this.leftEndpoint.merge(other.getLeftEndpoint)
    val mergedRightEndpoint: SingleEndpointConstruct =
      this.rightEndpoint.merge(other.getRightEndpoint)

    super.merge[DoubleEndpointConstruct](
      other,
      builder = (ref, copyPattern, groupDeclaration, expr) =>
        EdgeConstruct(
          ref, connType,
          mergedLeftEndpoint, mergedRightEndpoint,
          copyPattern, groupDeclaration, expr)
    )
  }
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
