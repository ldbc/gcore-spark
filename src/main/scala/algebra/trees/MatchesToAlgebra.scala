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

package algebra.trees

import algebra.expressions.{AlgebraExpression, Exists, Reference}
import algebra.operators.BinaryOperator.reduceLeft
import algebra.operators._
import common.trees.BottomUpRewriter

import scala.collection.mutable

/**
  * Converts all the remaining [[GcoreOperator]]s in the algebraic tree into
  * [[RelationalOperator]]s in order to obtain a fully relational tree. The entity relations are
  * still used as logical views of their respective tables.
  *
  * A [[CondMatchClause]] and an [[Exists]] sub-clause are transformed into relational trees as
  * follows:
  * - First, [[UnionAll]] subtrees are created from the non-optional [[SimpleMatchRelation]]s that
  * have the same [[BindingSet]].
  * - Next, relations (be they non-optional [[SimpleMatchRelation]]s or [[UnionAll]] relations
  * resulting from step 1) with common bindings in their [[BindingSet]] are [[InnerJoin]]ed.
  * - Relations (be they non-optional [[SimpleMatchRelation]]s or [[InnerJoin]]s from the previous
  * step) with disjoint [[BindingSet]]s are [[CrossJoin]]ed.
  * - Finally, the resulting binding table from the non-optional [[SimpleMatchRelation]]s is
  * [[LeftOuterJoin]]ed with the optional matches, from left to right as they appear in the query
  * (as per the language specification).
  *
  * A [[CondMatchClause]] is then transformed into a [[Select]] clause.
  */
object MatchesToAlgebra extends BottomUpRewriter[AlgebraTreeNode] {

  private val matchClause: RewriteFuncType = {
    case m: MatchClause =>
      reduceLeft(m.children.map(_.asInstanceOf[RelationLike]), LeftOuterJoin)
  }

  private val condMatchClause: RewriteFuncType = {
    case cm: CondMatchClause =>
      val simpleMatches: Seq[SimpleMatchRelation] =
        cm.children.init.map(_.asInstanceOf[SimpleMatchRelation])
      val where: AlgebraExpression = cm.children.last.asInstanceOf[AlgebraExpression]
      val matchesAfterUnion: Seq[RelationLike] = unionSimpleMatchRelations(simpleMatches)
      val joinedMatches: RelationLike = joinSimpleMatchRelations(matchesAfterUnion)
      Select(
        relation = joinedMatches,
        expr = where)
  }

  private val existsClause: RewriteFuncType = {
    case ec: Exists =>
      val simpleMatches: Seq[SimpleMatchRelation] =
        ec.children.map(_.asInstanceOf[SimpleMatchRelation])
      val matchesAfterUnion: Seq[RelationLike] = unionSimpleMatchRelations(simpleMatches)
      val joinedMatches: RelationLike = joinSimpleMatchRelations(matchesAfterUnion)

      ec.children = Seq(joinedMatches)
      ec
  }

  private type BindingToRelations = mutable.HashMap[Reference, mutable.Set[RelationLike]]
    with mutable.MultiMap[Reference, RelationLike]
  private type BsetToBindings = mutable.HashMap[Set[Reference], mutable.Set[RelationLike]]
    with mutable.MultiMap[Set[Reference], RelationLike]

  /** Creates a [[UnionAll]] of the [[RelationLike]]s with the same [[BindingSet]]. */
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

  /**
    * [[InnerJoin]]s [[RelationLike]]s with common bindings and [[CrossJoin]]s [[RelationLike]]s
    * with disjoint [[BindingSet]]s.
    */
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

  override val rule: RewriteFuncType = matchClause orElse condMatchClause orElse existsClause
}
