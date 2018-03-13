package algebra.trees

import algebra.exceptions.UnsupportedOperation
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
object GcoreToJoinGraph extends BottomUpRewriter[AlgebraTreeNode] {

  val idAttr: Attribute = Attribute(Reference("id"))
  val fromIdAttr: Attribute = Attribute(Reference("fromId"))
  val toIdAttr: Attribute = Attribute(Reference("toId"))

  override def rule: RewriteFuncType =
    matchClause orElse
      condMatchClause orElse
      simpleMatchClause orElse
      graphPattern orElse
      edge orElse
      vertex orElse
      withLabels

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
    * the "id" attribute into that of the binding. In the second case, we [[FullOuterJoin]] all the
    * [[RelationLike]]s wrapping the [[Edge]]s. The final binding table of the resulting
    * [[RelationLike]] is the union of all binding tables participating in the join.
    *
    * Note:
    * The syntax of a [[GraphPattern]] in a G-CORE query is:
    *
    *     MATCH (v1)->(v2)->(v3) ..., where the connections between the vertices can be of any type.
    *
    * Alongside a [[Graph]], this will form a [[SimpleMatchClause]]. The syntax of a
    * [[GraphPattern]] is different from that of a [[CondMatchClause]], where the patterns are
    * separated by a comma and need not necessarily contain common bindings:
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
        binaryOp = FullOuterJoin)
  }

  /**
    * The [[Vertex]] node contains a binding (a [[Reference]]) for the [[ObjectPattern]] of this
    * entity's. The [[RelationLike]] resulting from solving the [[ObjectPattern]] contains a series
    * of [[Attribute]]s pertaining to this [[Vertex]]. In the binding table created from the
    * [[Vertex]] node we are only interested in the unique identity of this entity, thus this node
    * is translated to a [[Projection]] of the "id" [[Attribute]] in this [[RelationLike]].
    *
    * Note: At this step, we create a [[BindingTable]] to hold the [[Reference]] of this entity.
    */
  private val vertex: RewriteFuncType = {
    case Vertex(ref, objPattern) =>
      VertexRelation(
        ref,
        RelationLike.empty,
        newBtable(
          ref,
          objPattern,
          rel => VertexRelation(ref, rel, new BindingTable(Map(ref -> rel)))))
  }

  /**
    * The [[Edge]] represents connection between two [[Vertex]] nodes. At this level in the tree,
    * each [[Vertex]] has been translated into its specific [[RelationLike]], having its own
    * [[BindingTable]]. The [[Edge]] itself has an [[ObjectPattern]] and a binding through its
    * [[Reference]]. We call this the binding table of the [[Edge]].
    *
    * The [[Edge]] node then becomes:
    *
    * <ul>
    *   <li> If the [[ConnectionType]] is either [[InConn]] or [[OutConn]], an [[EquiJoin]] between
    *   the binding table of the [[Edge]] and sequentially that of the source [[Vertex]]'s and of
    *   the destination [[Vertex]]'s. The resulting [[BindingTable]] of the [[EquiJoin]] will then
    *   be the binding table of the join.
    *
    *   <li> If the [[ConnectionType]] is either [[InOutConn]] or [[UndirectedConn]] (meaning that
    *   we can traverse this connection either from left to right or from right to left), the
    *   [[UnionAll]] of the [[RelationLike]]s resulting from solving this [[Edge]] with a
    *   [[ConnectionType]] first as the [[InConn]], then as the [[OutConn]]. The resulting
    *   [[BindingTable]] of the [[UnionAll]] (which will be the [[BindingTable]] of the two
    *   [[EquiJoin]] operands of the union) will then be the binding table of the result.
  *   </ul>
    */
  private val edge: RewriteFuncType = {
    case e @ Edge(ref, _, _, connType, objPattern) =>
      val leftEndpointRel: EntityRelation = e.children(1).asInstanceOf[EntityRelation]
      val rightEndpointRel: EntityRelation = e.children(2).asInstanceOf[EntityRelation]
      val edgeRel: RelationLike =
        EdgeRelation(
          ref,
          RelationLike.empty,
          newBtable(
            ref,
            objPattern,
            rel => EdgeRelation(ref, rel, new BindingTable(Map(ref -> rel)))))

      if (connType == InConn() || connType == OutConn()) {
        bindingsForInOrOutConn(leftEndpointRel, rightEndpointRel, edgeRel, connType)
      } else {
        val leftEdgeRightBindings: RelationLike =
          bindingsForInOrOutConn(leftEndpointRel, rightEndpointRel, edgeRel, OutConn())
        val rightEdgeLeftBindings: RelationLike =
          bindingsForInOrOutConn(leftEndpointRel, rightEndpointRel, edgeRel, InConn())

        UnionAll(
          lhs = leftEdgeRightBindings,
          rhs = rightEdgeLeftBindings)
      }
  }

  /**
    * [[WithLabels]] is a conjunction of disjunctions ([[HasLabel]]s) of [[Label]]s. Label
    * conjunction is not supported at the moment in queries.
    */
  private val withLabels: RewriteFuncType = {
    case WithLabels(And(HasLabel(_), HasLabel(_))) =>
      throw UnsupportedOperation("Label conjunction is not supported. An entity must have " +
        "only one label associated with it.")
    case WithLabels(And(hl @ HasLabel(_), True())) => hl
    case WithLabels(And(True(), hl @ HasLabel(_))) => hl
  }

  private def newBtable(ref: Reference,
                        objPattern: ObjectPattern,
                        makeRel: RelationLike => EntityRelation): BindingTable = {
    val propsPred: AlgebraExpression = objPattern.children.last.asInstanceOf[AlgebraExpression]
    val btable: BindingTable = new BindingTable(ref)

    objPattern match {
      case omp @ ObjectPattern(True(), _) =>
        val bindingTable: BindingTable = BindingTable.newEmpty
        bindingTable.btable.addBinding(
          key = ref,
          value =
            makeRel(
              Select(relation = AllRelations(), expr = propsPred, bindingTable = Some(btable))))
        bindingTable
      case _ =>
        val bindingTable: BindingTable = BindingTable.newEmpty
        val labelsPred: HasLabel = objPattern.children.head.asInstanceOf[HasLabel]
        labelsPred.children.foreach(label => {
          val labelRel: RelationLike = Relation(label.asInstanceOf[Label])
          bindingTable.btable.addBinding(
            key = ref,
            value =
              makeRel(Select(relation = labelRel, expr = propsPred, bindingTable = Some(btable))))
        })
        bindingTable
    }
  }

  private def bindingsForInOrOutConn(leftEndpointRel: RelationLike,
                                     rightEndpointRel: RelationLike,
                                     edgeRel: RelationLike,
                                     connType: ConnectionType): RelationLike = {
    val edgeRelJoinedOnFrom: RelationLike =
      EquiJoin(
        lhs = edgeRel,
        rhs = connType match {
          case InConn() => rightEndpointRel
          case OutConn() => leftEndpointRel
        },
        lhsAttribute = idAttr,
        rhsAttribute = fromIdAttr)

    val edgeRelJoinedOnFromAndTo: RelationLike =
      EquiJoin(
        lhs = edgeRelJoinedOnFrom,
        rhs = connType match {
          case InConn() => leftEndpointRel
          case OutConn() => rightEndpointRel
        },
        lhsAttribute = idAttr,
        rhsAttribute = toIdAttr)

    edgeRelJoinedOnFromAndTo
  }

  private type BindingToRelations = mutable.HashMap[Reference, mutable.Set[RelationLike]]
    with mutable.MultiMap[Reference, RelationLike]

  private def joinSimpleMatchRelations(relations: Seq[SimpleMatchRelation]): RelationLike = {
    val bindingToRelationMmap: BindingToRelations =
      new mutable.HashMap[Reference, mutable.Set[RelationLike]]
        with mutable.MultiMap[Reference, RelationLike]

    relations.foreach(relation => {
      val bset: Set[Reference] = relation.getBindingTable.bindingSet
      bset.foreach(ref => bindingToRelationMmap.addBinding(ref, relation))
    })

    joinSimpleMatchRelations(bindingToRelationMmap)
  }

  private def joinSimpleMatchRelations(bindingToRelationMmap: BindingToRelations)
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
      val join: RelationLike = reduceLeft(joinedRelations, FullOuterJoin)

      // For each binding in the join, remove previous relations it appeared in and are now part of
      // the joined relations, and add the join result to its mmap set.
      join.asInstanceOf[JoinLike].getBindingTable.bindingSet.foreach(
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
