package algebra.trees

import algebra.expressions.{AlgebraExpression, And, Reference}
import algebra.operators.{BasicConstructClause, CondConstructClause}
import algebra.types.{ConnectionConstruct, ConstructPattern, DoubleEndpointConstruct, SingleEndpointConstruct}
import common.trees.TopDownRewriter

import scala.collection.mutable

/**
  * Rewrites the CONSTRUCT sub-tree, by coalescing [[BasicConstructClause]]s that share variables.
  *
  * A [[BasicConstructClause]] contains a [[ConstructPattern]] of vertices or edges. If two such
  * patterns share variables, they must be combined into a single pattern, in order to avoid
  * constructing a re-occurring entity multiple times. This rewrite phase detects the set of
  * [[BasicConstructClause]]s that have common variables and joins each such set into a single
  * [[BasicConstructClause]]. The [[ConstructPattern]] of the new clause will become the set union
  * of all the [[ConstructPattern]]s in the clause set, while the condition of the binding table
  * becomes the conjunction ([[And]]) of all the conditions in the clause set.
  *
  * Note: Both the coalesced [[ConstructPattern]] and binding table condition are built under set
  * union, therefore duplicate patterns or conditions will be eliminated in the resulting
  * [[BasicConstructClause]].
  *
  * Examples of non-disjoint patterns:
  * > CONSTRUCT (a)->(b), (b)->(c) => CONSTRUCT (a)->(b)->(c)
  * > CONSTRUCT (a)->(b)->(c), (b)->(e) => CONSTRUCT (a)->(b)->{(c), (e)}
  */
object NormalizeBasicConstructs extends TopDownRewriter[AlgebraTreeNode] {

  private type RefSet = Set[Reference]
  private type RefToConstructMmap =
    mutable.HashMap[RefSet, mutable.Set[BasicConstructClause]]
      with mutable.MultiMap[RefSet, BasicConstructClause]

  override val rule: RewriteFuncType = {
    case condConstructClause: CondConstructClause =>
      val basicConstructs: Seq[AlgebraTreeNode] = condConstructClause.children
      val refMmap: RefToConstructMmap = extractRefMmap(basicConstructs)
      val coalescedConstructs: Seq[BasicConstructClause] =
        coalesceBasicConstructs(refMmap)
          .values
          .map(nonDisjointBasicConstructs => {
            val connectionConstructs: Set[ConnectionConstruct] =
              nonDisjointBasicConstructs.flatMap(
                basicConstruct => basicConstruct.constructPattern.topology).toSet
            val whens: Seq[AlgebraExpression] = nonDisjointBasicConstructs.map(_.when).toSeq

            BasicConstructClause(
              ConstructPattern(topology = connectionConstructs.toSeq),
              whens.reduce(And)
            )
          })
          .toSeq

      condConstructClause.children = coalescedConstructs
      condConstructClause
  }

  private def newRefToConstructMmap: RefToConstructMmap =
    new mutable.HashMap[RefSet, mutable.Set[BasicConstructClause]]
      with mutable.MultiMap[RefSet, BasicConstructClause]

  private def extractRefMmap(basicConstructs: Seq[AlgebraTreeNode]): RefToConstructMmap = {
    val refMmap: RefToConstructMmap = newRefToConstructMmap
    basicConstructs
      .foreach {
        case basicConstruct: BasicConstructClause =>
          val constructPattern: ConstructPattern = basicConstruct.constructPattern
          val topology: Seq[ConnectionConstruct] = constructPattern.topology
          val refs: Set[Reference] =
            topology.foldLeft(Set[Reference]()) {
              case (agg, vertex: SingleEndpointConstruct) =>
                agg + vertex.getRef
              case (agg, edgeOrPath: DoubleEndpointConstruct) =>
                agg ++ Set(
                  edgeOrPath.getRef,
                  edgeOrPath.getLeftEndpoint.getRef, edgeOrPath.getRightEndpoint.getRef)
            }
          refMmap.addBinding(refs, basicConstruct)
      }

    refMmap
  }

  private def coalesceBasicConstructs(refMmap: RefToConstructMmap): RefToConstructMmap = {
    val accumulator: RefToConstructMmap = newRefToConstructMmap
    val changed: Boolean = coalesceBasicConstructs(accumulator, refMmap)
    if (changed)
      coalesceBasicConstructs(accumulator)
    else
      accumulator
  }

  private def coalesceBasicConstructs(accumulator: RefToConstructMmap,
                                      refMmap: RefToConstructMmap): Boolean = {
    var changed: Boolean = false

    refMmap.foreach {
      case (refSet, basicConstructs) =>
        // Check if any of the reference sets (keys) in the accumulator contains at least one
        // reference from this (key, value) pair. Return the first such set, if there is any.
        val nonDisjointTuple: Option[(RefSet, mutable.Set[BasicConstructClause])] =
          accumulator.collectFirst {
            case (accRefSet, constructs) if accRefSet.intersect(refSet).nonEmpty =>
              (accRefSet, constructs)
          }

        if (nonDisjointTuple.isDefined) {
          // If there was such a reference set, then we need to coalesce the two sets, the one in
          // the accumulator and the one in refMmap. We add this new set to the accumulator and
          // remove the disjoint set we have just found.
          val nonDisjointRefSet: RefSet = nonDisjointTuple.get._1
          val nonDisjointConstructs: mutable.Set[BasicConstructClause] = nonDisjointTuple.get._2
          accumulator -= nonDisjointRefSet
          accumulator +=
            Tuple2(nonDisjointRefSet ++ refSet, nonDisjointConstructs ++ basicConstructs)

          // This means we have changed an entry in the accumulator.
          changed = true
        } else {
          // Otherwise, we add this tuple to the accumulator as is.
          accumulator += Tuple2(refSet, basicConstructs)
        }
    }

    changed
  }
}
