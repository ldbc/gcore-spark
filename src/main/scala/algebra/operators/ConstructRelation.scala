package algebra.operators

import algebra.expressions.{ObjectConstructPattern, PropertyRef, Reference}
import algebra.trees.AlgebraTreeNode
import algebra.types.ConnectionType
import common.compiler.Context
import schema.PathPropertyGraph

/**
  * A relation that holds the data of a new graph entity, denoted by its [[reference]]. This data
  * can then be used to add the respective entity to its corresponding [[PathPropertyGraph]].
  *
  * The relation dictates how the entity should be built starting from the binding table. New
  * properties can be added to the entity through its [[ObjectConstructPattern]] or through the
  * [[SetClause]]. Labels can be added through the [[ObjectConstructPattern]]. The [[RemoveClause]]
  * can be used to remove properties and/or labels. The [[PropertyRef]]erences used for grouping the
  * binding table (if there were any) are exposed through the [[groupedAttributes]] parameter.
  */
case class ConstructRelation(reference: Reference,
                             isMatchedRef: Boolean,
                             relation: RelationLike,
                             groupedAttributes: Seq[PropertyRef] = Seq.empty,
                             expr: ObjectConstructPattern,
                             setClause: Option[SetClause],
                             removeClause: Option[RemoveClause])
  extends UnaryOperator(relation) {

  children = List(reference, relation, expr) ++ groupedAttributes ++
    setClause.toList ++ removeClause.toList

  override def name: String = s"${super.name} [isMatchedRef = $isMatchedRef]"
}

abstract class EntityCreateRule extends GcoreOperator {
  override def checkWithContext(context: Context): Unit = {}
}

/** Shows which [[reference]] to use in a construct table to build a new vertex. */
case class VertexCreate(reference: Reference) extends EntityCreateRule {

  children = List(reference)

  override def name: String = s"${super.name} [${reference.refName}]"
}

/**
  * Shows which [[reference]] to use in a construct table to build a new edge. Given that an edge
  * identity is strictly determined by the identity of its endpoints, this [[EntityCreateRule]] also
  * holds its endpoin [[Reference]]s, as well as the [[ConnectionType]] between them.
  */
case class EdgeCreate(reference: Reference,
                      leftReference: Reference,
                      rightReference: Reference,
                      connType: ConnectionType) extends EntityCreateRule {

  children = List(reference, leftReference, rightReference, connType)

  override def name: String =
    s"${super.name} " +
      s"[${reference.refName}, ${leftReference.refName}, ${rightReference.refName}, $connType]"
}

/**
  * Constructs all the entities in a [[BasicConstructClause]].
  *
  * The construction starts from a [[baseConstructTable]] which is wrapped into a [[TableView]]. The
  * vertices are first constructed into the [[vertexConstructTable]], wrapped into its own
  * [[TableView]]. The edges are then constructed from the vertex table. Finally, the
  * [[createRules]] show how to extract the newly creates entities from the resulting
  * [[edgeConstructTable]].
  */
case class GroupConstruct(baseConstructTable: RelationLike,
                          vertexConstructTable: RelationLike,
                          baseConstructViewName: String,
                          vertexConstructViewName: String,
                          edgeConstructTable: RelationLike,
                          createRules: Seq[EntityCreateRule]) extends GcoreOperator {

  children = List(baseConstructTable, vertexConstructTable, edgeConstructTable) ++ createRules

  override def checkWithContext(context: Context): Unit = {}

  override def name: String = s"${super.name} [$baseConstructViewName, $vertexConstructViewName]"

  def getBaseConstructTable: AlgebraTreeNode = children.head

  def getVertexConstructTable: AlgebraTreeNode = children(1)

  def getEdgeConstructTable: AlgebraTreeNode = children(2)
}
