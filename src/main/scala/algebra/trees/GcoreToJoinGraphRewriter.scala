package algebra.trees

import algebra.expressions._
import algebra.operators.BinaryPrimitive.reduceLeft
import algebra.operators._
import algebra.types._
import common.trees.BottomUpRewriter

import scala.collection.mutable

/**
  * Transforms the [[GcorePrimitive]]s within the algebraic tree into a mix of common
  * [[AlgebraPrimitive]]s.
  */
object GcoreToJoinGraphRewriter extends BottomUpRewriter[AlgebraTreeNode] {

  override def rule: RewriteFuncType =
    matchClause orElse
      condMatchClause orElse
      simpleMatchClause orElse
      graphPattern orElse
      edge orElse
      vertex

  /**
    * A [[MatchClause]] is expressed between a [[CondMatchClause]] and zero or more optional
    * [[CondMatchClause]]s. This node is translated into a [[LeftOuterJoin]], from left to right,
    * on all the [[CondMatchClause]]s involved in the [[MatchClause]].
    */
  private val matchClause: RewriteFuncType = {
    case m @ MatchClause(_, _) =>
      reduceLeft(m.children.map(_.asInstanceOf[RelationLike]), LeftOuterJoin)
  }

  /**
    * A [[CondMatchClause]] is a [[MatchLike]] clause between a sequence of [[SimpleMatchClause]]s,
    * under the predicate of an [[AlgebraExpression]]. The [[SimpleMatchClause]]s may or may not
    * share bindings between them. If no bindings are shared, then this node is translated into a
    * [[CartesianProduct]] of the [[RelationLike]]s wrapping the [[SimpleMatchClause]]s. Otherwise,
    * the clauses that share bindings are [[InnerJoin]]ed and then the [[CartesianProduct]]
    * operator is applied over the join and the remaining clauses that have not participated in the
    * join. Finally, the results are filtered under the given predicate, with a [[Select]] clause.
    */
  private val condMatchClause: RewriteFuncType = {
    case cm @ CondMatchClause(_, _) =>
      val simpleMatches: Seq[SimpleMatchRelation] =
        cm.children.init.map(_.asInstanceOf[SimpleMatchRelation])
      val where: AlgebraExpression = cm.children.last.asInstanceOf[AlgebraExpression]
      val joinedMatches: RelationLike = joinSimpleMatchRelations(simpleMatches)
      Select(
        relation = joinedMatches,
        expr = where)
  }

  /**
    * A [[SimpleMatchClause]] is translated into a [[SimpleMatchRelation]] so as to pass on the
    * [[RelationLike]] that wraps the [[GraphPattern]] in this [[SimpleMatchClause]] alongside a
    * [[SimpleMatchRelationContext]] that retains the information about the [[Graph]] this pattern
    * is searched against.
    */
  private val simpleMatchClause: RewriteFuncType = {
    case m @ SimpleMatchClause(_, _) =>
      val relation: RelationLike = m.children.head.asInstanceOf[RelationLike]
      SimpleMatchRelation(
        relation = relation,
        context = SimpleMatchRelationContext(m.children.last.asInstanceOf[Graph]))
  }

  /**
    * A [[GraphPattern]] is either only a [[Vertex]] pattern or a sequence of one or more [[Edge]]
    * patterns. In case it is only a [[Vertex]] pattern, it translates to the [[RelationLike]]
    * wrapping the [[Vertex]] on top of which we add a [[Rename]] operation, to change the name of
    * the "id" attribute into that of the binding. In the second case, we [[InnerJoin]] all the
    * [[RelationLike]]s wrapping the [[Edge]]s. The final binding table of the resulting
    * [[RelationLike]] is the union of all binding tables participating in the join.
    *
    * Note:
    * The syntax of a [[GraphPattern]] in a G-CORE query is:
    *
    *     MATCH (v1)->(v2)->(v3) ..., where the connections between the vertices can be of any type.
    *
    * Alongside a [[Graph]], this will form a [[SimpleMatchClause]]. The syntax of a
    * [[GraphPattern]] is fundamentally different from that of a [[CondMatchClause]], where the
    * patterns are separated by a comma and need not necessarily contain common bindings:
    *
    *     MATCH (v1)->(v2), (v3) ..., where the connections between the vertices can be of any type.
    *
    * The query above contains two [[SimpleMatchClause]]s, each with their own [[GraphPattern]]:
    *
    *     (v1)->(v2) and (v3)
    */
  private val graphPattern: RewriteFuncType = {
    case gp @ GraphPattern(_) =>
      reduceLeft(
        relations = gp.children.map(_.asInstanceOf[RelationLike]),
        binaryOp = InnerJoin)
  }

  /**
    * The [[Vertex]] node contains a binding (a [[Reference]]) for the [[ObjectPattern]] of this
    * entity's. The [[RelationLike]] resulting from solving the [[ObjectPattern]] contains a series
    * of [[Attribute]]s pertaining to this [[Vertex]]. In the binding table created from the
    * [[Vertex]] node we are only interested in the unique identity of this entity, thus this node
    * is translated to a [[Projection]] of the "id" [[Attribute]] in this [[RelationLike]].
    *
    * Note: At this step, we create a [[BindingContext]] to hold the [[Reference]] of this entity.
    */
  private val vertex: RewriteFuncType = {
    case v @ Vertex(ref, _) =>
      EntityRelation(
        ref = ref,
        objPattern = v.children(1).asInstanceOf[ObjectPattern])
  }

  /**
    * The [[Edge]] represents connection between two [[Vertex]] nodes. At this level in the tree,
    * each [[Vertex]] has been translated into its specific [[RelationLike]], having its own
    * [[BindingContext]]. The [[Edge]] itself has an [[ObjectPattern]] and a binding through its
    * [[Reference]]. We call this the binding table of the [[Edge]].
    *
    * The [[Edge]] node then becomes:
    *
    * <ul>
    *   <li> If the [[ConnectionType]] is either [[InConn]] or [[OutConn]], an [[EquiJoin]] between
    *   the binding table of the [[Edge]] and sequentially that of the source [[Vertex]]'s and of
    *   the destination [[Vertex]]'s. The resulting [[BindingContext]] of the [[EquiJoin]] will then
    *   be the binding table of the join.
    *
    *   <li> If the [[ConnectionType]] is either [[InOutConn]] or [[UndirectedConn]] (meaning that
    *   we can traverse this connection either from left to right or from right to left), the
    *   [[UnionAll]] of the [[RelationLike]]s resulting from solving this [[Edge]] with a
    *   [[ConnectionType]] first as the [[InConn]], then as the [[OutConn]]. The resulting
    *   [[BindingContext]] of the [[UnionAll]] (which will be the [[BindingContext]] of the two
    *   [[EquiJoin]] operands of the union) will then be the binding table of the result.
  *   </ul>
    */
  private val edge: RewriteFuncType = {
    case e @ Edge(_, _, _, connType, _) =>
      if (connType == InConn() || connType == OutConn()) {
        bindingsForInOrOutConn(e, connType)
      } else {
        val leftEdgeRightBindings: RelationLike = bindingsForInOrOutConn(e, OutConn())
        val rightEdgeLeftBindings: RelationLike = bindingsForInOrOutConn(e, InConn())
        UnionAll(
          lhs = leftEdgeRightBindings,
          rhs = rightEdgeLeftBindings)
      }
  }

  private def bindingsForInOrOutConn(edge: AlgebraTreeNode,
                                     connType: ConnectionType): RelationLike = {
    val leftEndpointRel: EntityRelation = edge.children(1).asInstanceOf[EntityRelation]
    val rightEndpointRel: EntityRelation = edge.children(2).asInstanceOf[EntityRelation]
    val edgeRef: Reference = edge.children.head.asInstanceOf[Reference]
    val edgeRel: RelationLike =
      EntityRelation(
        ref = edgeRef,
        objPattern = edge.children.last.asInstanceOf[ObjectPattern])

    val edgeRelJoinedOnFrom: RelationLike =
      EquiJoin(
        lhs = edgeRel,
        rhs = connType match {
          case InConn() => rightEndpointRel
          case OutConn() => leftEndpointRel
        },
        lhsAttribute = Reference("fromId"),
        rhsAttribute = Reference("id"))

    val edgeRelJoinedOnFromAndTo: RelationLike =
      EquiJoin(
        lhs = edgeRelJoinedOnFrom,
        rhs = connType match {
          case InConn() => leftEndpointRel
          case OutConn() => rightEndpointRel
        },
        lhsAttribute = Reference("toId"),
        rhsAttribute = Reference("id"))

    edgeRelJoinedOnFromAndTo
  }

  private type BindingToRelationMmap =
    mutable.HashMap[Reference, mutable.Set[RelationLike]]
      with mutable.MultiMap[Reference, RelationLike]

  private def joinSimpleMatchRelations(relations: Seq[SimpleMatchRelation]): RelationLike = {
    val bindingToRelationMmap: BindingToRelationMmap =
      new mutable.HashMap[Reference, mutable.Set[RelationLike]]
        with mutable.MultiMap[Reference, RelationLike]

    relations.foreach(relation => {
      val bset: BindingSet = relation.getBindingContext.bset
      bset.foreach(ref => bindingToRelationMmap.addBinding(ref, relation))
    })

    joinSimpleMatchRelations(bindingToRelationMmap)
  }

  private def joinSimpleMatchRelations(bindingToRelationMmap: BindingToRelationMmap)
  : RelationLike = {
    // The first binding that appears in more than one relation.
    val commonBindingOption: Option[(Reference, mutable.Set[RelationLike])] =
      bindingToRelationMmap.find(bindingToRelationSet => {
        bindingToRelationSet._2.size >= 2
      })

    if (commonBindingOption.isDefined) {
      // Extract a binding from the multimap that appears in more than one relation.
      val multiBinding: (Reference, mutable.Set[RelationLike]) = commonBindingOption.get

      // Do a natural join over relations that share the variable.
      val joinedRelations = multiBinding._2.toSeq
      val join: RelationLike = reduceLeft(joinedRelations, InnerJoin)

      // For each binding in the join, remove previous relations it appeared in and are now part of
      // the joined relations, and add the join result to its mmap set.
      join.asInstanceOf[JoinLike].getBindings.foreach(
        ref => {
          joinedRelations.foreach(
            // If this binding appeared in this relation, remove the relation from the mmap.
            // Otherwise, the operation does not have any effect on the mmap.
            relation => bindingToRelationMmap.removeBinding(ref, relation)
          )

          bindingToRelationMmap.addBinding(ref, join)
        }
      )

      // Call joinSimpleMatchRelations recursively,
      joinSimpleMatchRelations(bindingToRelationMmap)

    } else {
      // Every binding now has only one relation it appears in. The result is the cross-join of all
      // unique relations in the multimap.
      reduceLeft(
        relations =
          bindingToRelationMmap.foldLeft(Set.empty[RelationLike]) {
            (agg, bindingToRels) => agg.union(bindingToRels._2)
          }.toSeq,
        binaryOp = CartesianProduct)
    }
  }

}
