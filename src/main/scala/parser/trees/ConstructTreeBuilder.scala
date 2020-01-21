/*
 * gcore-spark is the reference implementation of the G-CORE graph query
 * language by the Linked Data Benchmark Council (LDBC) - ldbcouncil.org
 *
 * The copyrights of the source code in this file belong to:
 * - CWI (www.cwi.nl), 2017-2018
 * - Universidad de Talca (www.utalca.cl), 2018
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

package parser.trees

import algebra.expressions._
import algebra.operators
import algebra.operators._
import algebra.types._
import parser.exceptions.QueryParseException
import parser.trees.ExpressionTreeBuilder._
import parser.trees.QueryTreeBuilder._

import scala.collection.mutable

object ConstructTreeBuilder {

  /** Creates a [[ConstructClause]] from a given Construct node. */
  def extractConstructClause(from: SpoofaxBaseTreeNode): ConstructClause = {

    val constructs= from.children
    val constructExps: mutable.ArrayBuffer[ConstructExp] = new mutable.ArrayBuffer[ConstructExp]()

    constructs.foreach( constructPattern =>{
      val constructExp = constructPattern.children.head
      val constructWhere: mutable.ArrayBuffer[AlgebraExpression] = new mutable.ArrayBuffer[AlgebraExpression]()
      val constructHaving: mutable.ArrayBuffer[AlgebraExpression] = new mutable.ArrayBuffer[AlgebraExpression]()
      val graphs: mutable.ArrayBuffer[Graph] = new mutable.ArrayBuffer[Graph]()
      val constructClauses: mutable.ArrayBuffer[CondConstructClause] = new mutable.ArrayBuffer[CondConstructClause]()
      val propSets: mutable.ArrayBuffer[PropertySet] = new mutable.ArrayBuffer[PropertySet]()
      val propRemoves: mutable.ArrayBuffer[PropertyRemove] = new mutable.ArrayBuffer[PropertyRemove]()
      val labelRemoves: mutable.ArrayBuffer[LabelRemove] = new mutable.ArrayBuffer[LabelRemove]()

      constructExp.children.foreach {
        case namedGraph: SpoofaxLeaf[_] =>
          graphs += NamedGraph(namedGraph.asInstanceOf[SpoofaxLeaf[String]].leafValue)

        case queryGraph: SpoofaxTreeNode if queryGraph.name == "BasicQuery" =>
          graphs += QueryGraph(extractQueryClause(queryGraph))

        case whereCondition: SpoofaxTreeNode if whereCondition.name == "Some" && whereCondition.children.head.name == "ConstructWhere" =>
          constructWhere += extractWhen(whereCondition)

        case whereCondition: SpoofaxTreeNode if whereCondition.name == "None" => True

        case havingCondition: SpoofaxTreeNode if havingCondition.name == "Some" && havingCondition.children.head.name == "ConstructHaving" =>
          constructHaving += extractWhen(havingCondition)

        case havingCondition: SpoofaxTreeNode if havingCondition.name == "None" => True

        case basicConstruct: SpoofaxTreeNode if basicConstruct.name == "BasicConstructPattern" =>
          constructClauses += extractBasicConstructClause(basicConstruct)

        case propSet: SpoofaxTreeNode if propSet.name == "SetClause" =>
          propSets += extractPropertySet(propSet)

        case remove: SpoofaxTreeNode
          if (remove.name == "Property") || (remove.name == "Labels") =>

          val update: AlgebraExpression = extractRemoveClause(remove)
          update match {
            case propRemove: PropertyRemove => propRemoves += propRemove
            case labelRemove: LabelRemove => labelRemoves += labelRemove
          }

        case _ => throw QueryParseException(s"Unknown ConstructPattern child type ${from.name}")
      }
      constructExps+=
        ConstructExp(
          graphs = GraphUnion(graphs),
          condConstructs = CondConstructs(constructClauses),
          where = Where(constructWhere),
          setClause = SetClause(propSets),
          removeClause = RemoveClause(propRemoves, labelRemoves),
          having = Having(constructHaving)
        )
    })
    ConstructClause(
      constructExp = constructExps
    )
  }

  /** Creates a [[CondConstructClause]] from a given BasicConstructPattern node. */
  private def extractBasicConstructClause(from: SpoofaxBaseTreeNode): CondConstructClause = {
    from.name match {
      case "BasicConstructPattern" =>
        CondConstructClause(
          constructPattern =
            ConstructPattern(extractConstructTopology(from.children, prevRef = None)))
      case _ =>
        throw QueryParseException(
          s"Cannot extract CondConstructClause from node type ${from.name}")
    }
  }

  /** Extracts a [[PropertySet]] from a SetClause node. */
  private def extractPropertySet(from: SpoofaxBaseTreeNode): PropertySet = {
    from.name match {
      case "SetClause" =>
        val propertyRef = extractExpression(from.children.head).asInstanceOf[PropertyRef]
        val expr = extractExpression(from.children.last)
        PropertySet(
          ref = propertyRef.ref,
          propAssignment = PropAssignment(propertyRef.propKey, expr))
      case _ => throw QueryParseException(s"Cannot extract SetClause from node type ${from.name}")
    }
  }

  /** Extracts a [[PropertyRemove]] or a [[LabelRemove]] from a remove clause node. */
  private def extractRemoveClause(from: SpoofaxBaseTreeNode): AlgebraExpression = {
    from.name match {
      case "Property" =>
        PropertyRemove(extractExpression(from.children.head).asInstanceOf[PropertyRef])
      case "Labels" =>
        val varRef = from.children.head
        val labels = from.children.last.children
        LabelRemove(
          ref = extractExpression(varRef).asInstanceOf[Reference],
          labelAssignments = LabelAssignments(labels.map(extractExpression(_).asInstanceOf[Label]))
        )
      case _ =>
        throw QueryParseException(s"Cannot extract RemoveClause from node type ${from.name}")
    }
  }

  /** Helper method to extract the topology from the children of a BasicConstructPattern. */
  private def extractConstructTopology(from: Seq[SpoofaxBaseTreeNode],
                                       prevRef: Option[SingleEndpointConstruct])
  : Seq[ConnectionConstruct] = {
    from match {
      // Vertex
      case Seq(vertex) if vertex.name == "Vertex" => Seq(extractVertexConstruct(vertex))

      // Vertex - EdgeVertexConstructPattern - ...
      case Seq(vertex, connections@_*) if vertex.name == "Vertex" =>
        extractConstructTopology(
          /*from =*/    connections,
          /*prevRef =*/ Some(extractVertexConstruct(vertex)))

      // ... - EdgeVertexConstructPattern - ...
      case Seq(connection, connections@_*) if connection.name == "EdgeVertexConstructPattern" =>
        val extractedConnection: DoubleEndpointConstruct =
          extractConstructConnection(connection, prevRef.get)
        extractedConnection +:
          extractConstructTopology(connections, Some(extractedConnection.getRightEndpoint))

      case _ => Seq.empty
    }
  }

  /** Creates an algebraic [[VertexConstruct]] from a Vertex lexical node. */
  private def extractVertexConstruct(from: SpoofaxBaseTreeNode): SingleEndpointConstruct = {
    val refDef = from.children.head /*Some*/
      .children.head /*VarRefDef*/
    val copyPattern = from.children(1)
    val groupDeclaration = from.children(2)
    val objectConstructPattern = from.children.last

    val refName = refDef.children.head.asInstanceOf[SpoofaxLeaf[String]]

    VertexConstruct(
      ref = Reference(refName.value),
      copyPattern = extractCopyPattern(copyPattern),
      groupDeclaration = extractGroupDeclaration(groupDeclaration),
      expr = extractObjectConstructPattern(objectConstructPattern))
  }

  private def extractConstructConnection(from: SpoofaxBaseTreeNode,
                                         prevRef: SingleEndpointConstruct)
  : DoubleEndpointConstruct = {
    val connElement = from.children.head /*connType*/
      .children.head /*Some*/
      .children.head /*Edge or Path*/

    connElement.name match {
      case "Edge" => extractEdgeConstruct(from, prevRef)
      case "PathVirtual" => extractVirtualPathConstruct(from, prevRef)
      case "PathObjectified" => extractStoredPathConstruct(from, prevRef)
    }
  }

  /** Creates an algebraic [[EdgeConstruct]] from an Edge lexical node. */
  private def extractEdgeConstruct(from: SpoofaxBaseTreeNode,
                                   prevRef: SingleEndpointConstruct): EdgeConstruct = {
    val connType = from.children.head.name match {
      case "InConn" => InConn
      case "OutConn" => OutConn
      case "InOutEdge" => InOutConn
    }

    val connElement =
      from.children.head /*connType*/
        .children.head /*Some*/
        .children.head /*Edge*/
    val edgeConstructPattern = connElement.children.head

    val refDef = edgeConstructPattern.children.head.children.head // Some(VarRefDef(...))
    val copyPattern = edgeConstructPattern.children(1)
    val groupDeclaration = edgeConstructPattern.children(2)
    val objectConstructPattern = edgeConstructPattern.children.last

    val refName = refDef.children.head.asInstanceOf[SpoofaxLeaf[String]]
    val rightEndpoint = extractVertexConstruct(from.children(1))

    EdgeConstruct(
      connName = Reference(refName.value),
      connType = connType,
      leftEndpoint = prevRef, rightEndpoint = rightEndpoint,
      copyPattern = extractCopyPattern(copyPattern),
      groupDeclaration = extractGroupDeclaration(groupDeclaration),
      expr = extractObjectConstructPattern(objectConstructPattern))
  }

  /** Creates an algebraic [[StoredPathConstruct]] from an PathObjectified lexical node. */
  private def extractStoredPathConstruct(from: SpoofaxBaseTreeNode,
                                         prevRef: SingleEndpointConstruct): StoredPathConstruct = {
    val connType = from.children.head.name match {
      case "InConn" => InConn
      case "OutConn" => OutConn
      case "UndirectedEdge" => UndirectedConn
      case "InOutEdge" => InOutConn
    }

    val connElement =
      from.children.head /*connType*/
        .children.head /*Some*/
        .children.head /*PathObjectified*/

    val refDef = connElement.children.head.children.head // Some(VarRefDef(...))
    val copyPattern = connElement.children(1)
    val objectConstructPattern = connElement.children.last

    val refName = refDef.children.head.asInstanceOf[SpoofaxLeaf[String]]
    val rightEndpoint = extractVertexConstruct(from.children(1))

    StoredPathConstruct(
      connName = Reference(refName.value),
      connType = connType,
      leftEndpoint = prevRef, rightEndpoint = rightEndpoint,
      copyPattern = extractCopyPattern(copyPattern),
      expr = extractObjectConstructPattern(objectConstructPattern))
  }

  /** Creates an algebraic [[VirtualPathConstruct]] from an PathVirtual lexical node. */
  private
  def extractVirtualPathConstruct(from: SpoofaxBaseTreeNode,
                                  prevRef: SingleEndpointConstruct): VirtualPathConstruct = {
    val connType = from.children.head.name match {
      case "InConn" => InConn
      case "OutConn" => OutConn
      case "UndirectedEdge" => UndirectedConn
      case "InOutEdge" => InOutConn
    }

    val connElement =
      from.children.head /*connType*/
        .children.head /*Some*/
        .children.head /*PathVirtual*/

    val refDef = connElement.children.head // VarRefDef(...), name is mandatory here
    val refName = refDef.children.head.asInstanceOf[SpoofaxLeaf[String]]
    val rightEndpoint = extractVertexConstruct(from.children(1))

    VirtualPathConstruct(
      connName = Reference(refName.value),
      connType = connType,
      leftEndpoint = prevRef, rightEndpoint = rightEndpoint)
  }

  /** Creates a [[Reference]] from a CopyPattern lexical node. */
  private def extractCopyPattern(from: SpoofaxBaseTreeNode): Option[Reference] = {
    from.name match {
      case "None" => None
      case "Some" =>
        Some(
          Reference(
            from.children.head // CopyPattern
              .children.head // VarRef
              .children.head.asInstanceOf[SpoofaxLeaf[String]].value))
    }
  }

  /** Extracts the [[GroupDeclaration]] from its GroupDeclaration lexical node. */
  private def extractGroupDeclaration(from: SpoofaxBaseTreeNode): Option[GroupDeclaration] = {
    from.name match {
      case "None" => None
      case "Some" => extractGroupDeclaration(from.children.head)
      case "GroupDeclaration" =>
        Some(GroupDeclaration(from.children.map(extractExpression)))
      case _ =>
        throw QueryParseException(s"Cannot extract GroupDeclaration from node type ${from.name}")
    }
  }

  /**
    * Creates the [[ObjectConstructPattern]] of an entity from the ObjectConstructPattern lexical
    * node.
    */
  private def extractObjectConstructPattern(from: SpoofaxBaseTreeNode): ObjectConstructPattern = {
    from.name match {
      case "Some" => extractObjectConstructPattern(from.children.head)
      case "None" => ObjectConstructPattern(LabelAssignments(Seq.empty), PropAssignments(Seq.empty))
      case "ObjectConstructPattern" =>
        val labelAssignments = from.children.head
        val propAssignments = from.children.last

        ObjectConstructPattern(
          extractLabelAssignments(labelAssignments),
          extractPropAssignments(propAssignments))
      case _ =>
        throw QueryParseException(s"Cannot extract ObjectConstructPattern from ${from.name}")
    }
  }

  private def extractLabelAssignments(from: SpoofaxBaseTreeNode): LabelAssignments = {
    from.name match {
      case "None" => LabelAssignments(Seq.empty)
      case "Some" => extractLabelAssignments(from.children.head)
      case "Labels" => LabelAssignments(from.children.map(extractExpression(_).asInstanceOf[Label]))
      case _ =>
        throw QueryParseException(s"Cannot extract LabelAssignements from node type ${from.name}")
    }
  }

  private def extractPropAssignments(from: SpoofaxBaseTreeNode): PropAssignments = {
    from.name match {
      case "None" => PropAssignments(Seq.empty)
      case "Some" => extractPropAssignments(from.children.head)
      case "Props" =>
        /** The first and last child of the Props node are { and }, respectively. */
        PropAssignments(
          from.children.tail.init.map(extractExpression(_).asInstanceOf[PropAssignment]))
      case _ =>
        throw QueryParseException(s"Cannot extract PropAssignements from node type ${from.name}")
    }
  }

  /** Extracts the condition of a [[CondConstructClause]] from a ConstructCondition node. */
  private def extractWhen(from: SpoofaxBaseTreeNode): AlgebraExpression = {
    from.name match {
      case "None" => True
      // Some(Where(...)) for match or Some(ConstructCondition(...)) for construct
      case "Some" => extractWhen(from.children.head)
      case "ConstructWhere" => extractExpression(from.children.head)
      case "ConstructHaving" => extractExpression(from.children.head)
      case _ => throw QueryParseException(s"Cannot extract Expression from node type ${from.name}")
    }
  }
}
