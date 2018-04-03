package algebra.trees

import algebra.expressions.{AlgebraExpression, Reference}
import algebra.operators.BinaryPrimitive.reduceLeft
import algebra.operators._
import common.trees.BottomUpRewriter

import scala.collection.mutable

object MatchesToAlgebra extends BottomUpRewriter[AlgebraTreeNode] {

  private val matchClause: RewriteFuncType = {
    case m @ MatchClause(_, _) =>
      reduceLeft(m.children.map(_.asInstanceOf[RelationLike]), LeftOuterJoin)
  }

  private val condMatchClause: RewriteFuncType = {
    case cm @ CondMatchClause(_, _) =>
      val simpleMatches: Seq[SimpleMatchRelation] =
        cm.children.init.map(_.asInstanceOf[SimpleMatchRelation])
      val where: AlgebraExpression = cm.children.last.asInstanceOf[AlgebraExpression]
      val matchesAfterUnion: Seq[RelationLike] = unionSimpleMatchRelations(simpleMatches)
      val joinedMatches: RelationLike = joinSimpleMatchRelations(matchesAfterUnion)
      Select(
        relation = joinedMatches,
        expr = where)
  }

  private type BindingToRelations = mutable.HashMap[Reference, mutable.Set[RelationLike]]
    with mutable.MultiMap[Reference, RelationLike]
  private type BsetToBindings = mutable.HashMap[Set[Reference], mutable.Set[RelationLike]]
    with mutable.MultiMap[Set[Reference], RelationLike]

  private def unionSimpleMatchRelations(relations: Seq[SimpleMatchRelation]): Seq[RelationLike] = {
    val relationToBindingMmap: BsetToBindings =
      new mutable.HashMap[Set[Reference], mutable.Set[RelationLike]]
        with mutable.MultiMap[Set[Reference], RelationLike]

    // Find those SimpleMatches that share the binding set - this will be either the binding of a
    // vertex, or the three-set for an edge (from, edge, to).
    relations.foreach(relation => {
      val bset: Set[Reference] = relation.getBindingSet.refSet
      relationToBindingMmap.addBinding(bset, relation)
    })

    // For binding sets that share at least two SimpleMatches, replace their relations with the
    // union of them.
    relationToBindingMmap
      .filter(kv => {
        val relations: mutable.Set[RelationLike] = kv._2
        relations.size >= 2
      })
      .foreach(kv => {
        val bindingSet: Set[Reference] = kv._1
        val relations: mutable.Set[RelationLike] = kv._2

        val unionAll: RelationLike = reduceLeft(relations.toSeq, UnionAll)
        relationToBindingMmap.remove(bindingSet)
        relationToBindingMmap.addBinding(bindingSet, unionAll)
      })

    // Return all relations that remain in the map. Now each binding set will correspond to exactly
    // one relation in the map.
    relationToBindingMmap.values.flatten.toSeq
  }

  private def joinSimpleMatchRelations(relations: Seq[RelationLike]): RelationLike = {
    val bindingToRelationMmap: BindingToRelations =
      new mutable.HashMap[Reference, mutable.Set[RelationLike]]
        with mutable.MultiMap[Reference, RelationLike]

    relations.foreach(relation => {
      val bset: Set[Reference] = relation.getBindingSet.refSet
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
      val join: RelationLike = reduceLeft(joinedRelations, InnerJoin)

      // For each binding in the join, remove previous relations it appeared in and are now part of
      // the joined relations, and add the join result to its mmap set.
      join.asInstanceOf[JoinLike].getBindingSet.refSet.foreach(
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
        binaryOp = CrossJoin)
    }
  }

  override val rule: RewriteFuncType = matchClause orElse condMatchClause
}
