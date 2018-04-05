package algebra.trees

import algebra.expressions.{Exists, Label, Reference}
import algebra.operators._
import algebra.types.{Connection, DefaultGraph, Graph, GraphPattern, NamedGraph}
import common.trees.TopDownRewriter
import schema.{GraphDb, GraphSchema, SchemaMap}

import scala.collection.mutable

/**
  * A rewriting phase that does label inference on all the variables used in the [[MatchClause]],
  * including:
  * - variables used in the non-optional [[CondMatchClause]];
  * - variables used in the optional [[CondMatchClause]]s (if any);
  * - variables used in the [[Exists]] sub-queries (if any).
  *
  * Label inference is the process of finding the minimal set of [[Label]]s that determine a
  * variable, given all the [[Connection]]s in all the graph patterns of a match clause. In other
  * words, it is the process of identifying which tables will be queried for a particular variable
  * in the [[MatchClause]]. It is essential to find the minimal set of labels/tables, as the runtime
  * of solving a query is directly related to the size of the tables participating in the relational
  * operations.
  *
  * Label inference relies on previous rewrite phases, where entity relations
  * ([[VertexRelation]], [[EdgeRelation]], [[StoredPathRelation]]) have been either labeled with a
  * fixed label ([[Relation]]), if this was provided in the query, or with the label
  * [[AllRelations]], which means that at that point in the rewrite pipeline, we could only infer
  * that the data for the respective variable is to be queried in all available tables for its type.
  *
  * During the inference process, each [[Connection]] in a [[GraphPattern]] becomes a
  * [[SimpleMatchRelation]]. At the end of the inference process:
  * - if a [[Connection]] is strictly labeled (there is only one [[Label]] in the minimal set), then
  * we emit a single [[SimpleMatchRelation]] for that [[Connection]];
  * - otherwise, we emit as many [[SimpleMatchRelation]]s as the size of the minimal set and
  * preserve the binding tuple of the [[Connection]] in the newly formed relations.
  */
case class ExpandRelations(context: AlgebraContext) extends TopDownRewriter[AlgebraTreeNode] {

  sealed abstract class EntityTuple
  sealed case class VertexTuple(label: Label) extends EntityTuple
  sealed case class EdgeOrPathTuple(label: Label, fromLabel: Label, toLabel: Label)
    extends EntityTuple

  private type BindingToLabelsMmap = mutable.HashMap[Reference, mutable.Set[Label]]
    with mutable.MultiMap[Reference, Label]

  private type MatchToBindingTuplesMmap =
    mutable.HashMap[SimpleMatchRelation, mutable.Set[EntityTuple]]
      with mutable.MultiMap[SimpleMatchRelation, EntityTuple]

  private def newBindingToLabelsMmap: BindingToLabelsMmap =
    new mutable.HashMap[Reference, mutable.Set[Label]]
      with mutable.MultiMap[Reference, Label]

  private def newMatchToBindingsMmap: MatchToBindingTuplesMmap =
    new mutable.HashMap[SimpleMatchRelation, mutable.Set[EntityTuple]]
      with mutable.MultiMap[SimpleMatchRelation, EntityTuple]

  override val rule: RewriteFuncType = {
    case matchClause: MatchClause =>
      val graphDb: GraphDb = context.graphDb

      val allSimpleMatches: mutable.ArrayBuffer[SimpleMatchRelation] =
        new mutable.ArrayBuffer[SimpleMatchRelation]()

      // For both non-optional and optional match clauses, replace the SimpleMatchClauses with
      // SimpleMatchRelations and merge all SimpleMatchRelations into the allSimpleMatches buffer.
      // We want to perform label resolution on all patterns as a whole.
      matchClause.children.foreach(
        condMatch => {
          val simpleMatches: Seq[SimpleMatchRelation] =
            toMatchRelations(condMatch.asInstanceOf[CondMatchClause])
          val matchPred: AlgebraTreeNode = condMatch.children.last
          condMatch.children = simpleMatches :+ matchPred

          allSimpleMatches ++= simpleMatches

          // If the matching predicate contains an existence clause, then we need to add to the
          // allSimpleMatches buffer all the SimpleMatchRelations under the Exists condition.
          matchPred forEachDown {
            case e: Exists =>
              val existsMatches: Seq[SimpleMatchRelation] = toMatchRelations(e.children)
              e.children = existsMatches
              allSimpleMatches ++= existsMatches
            case _ =>
          }
        })

      // Use all SimpleMatchRelations to restrict the labels of all variables in the match query.
      val constrainedLabels: BindingToLabelsMmap = restrictLabelsOverall(allSimpleMatches, graphDb)

      // Constrain labels for each CondMatchClause.
      matchClause.children.foreach(
        condMatch => {
          val simpleMatches: Seq[SimpleMatchRelation] =
            constrainSimpleMatches(
              simpleMatches = condMatch.children.init, constrainedLabels, graphDb)
          val matchPred: AlgebraTreeNode = condMatch.children.last
          condMatch.children = simpleMatches :+ matchPred

          // If the matching predicate is an existence clause, we also constrain the simple matches
          // under the Exists condition.
          matchPred forEachDown {
            case e: Exists =>
              val existsMatches: Seq[SimpleMatchRelation] =
                constrainSimpleMatches(simpleMatches = e.children, constrainedLabels, graphDb)
              e.children = existsMatches
            case _ =>
          }
        })

      matchClause
  }

  private def toMatchRelations(condMatchClause: CondMatchClause): Seq[SimpleMatchRelation] = {
    toMatchRelations(condMatchClause.children.init)
  }

  private def toMatchRelations(simpleMatches: Seq[AlgebraTreeNode]): Seq[SimpleMatchRelation] = {
    simpleMatches.flatMap(
      simpleMatchClause => {
        val graphPattern: AlgebraTreeNode = simpleMatchClause.children.head
        val graph: Graph = simpleMatchClause.children.last.asInstanceOf[Graph]
        graphPattern.children.map(pattern =>
          SimpleMatchRelation(
            relation = pattern.asInstanceOf[RelationLike],
            context = SimpleMatchRelationContext(graph)
          ))
      })
  }

  private def constrainSimpleMatches(simpleMatches: Seq[AlgebraTreeNode],
                                     constrainedLabels: BindingToLabelsMmap,
                                     graphDb: GraphDb): Seq[SimpleMatchRelation] = {
    val constrainedPerMatch: MatchToBindingTuplesMmap =
      restrictLabelsPerMatch(simpleMatches, constrainedLabels, graphDb)

    simpleMatches.flatMap {
      case m @ SimpleMatchRelation(rel @ VertexRelation(_, _, _), _, _) =>
        constrainedPerMatch(m).map(tuple =>
          m.copy(
            relation = rel.copy(labelRelation = Relation(tuple.asInstanceOf[VertexTuple].label)))
        )
      case m @ SimpleMatchRelation(rel @ EdgeRelation(_, _, _, fromRel, toRel), _, _) =>
        constrainedPerMatch(m).map(tuple => {
          val edgeTuple: EdgeOrPathTuple = tuple.asInstanceOf[EdgeOrPathTuple]
          m.copy(
            relation = rel.copy(
              labelRelation = Relation(edgeTuple.label),
              fromRel = fromRel.copy(labelRelation = Relation(edgeTuple.fromLabel)),
              toRel = toRel.copy(labelRelation = Relation(edgeTuple.toLabel))))
        })
      case m @ SimpleMatchRelation(
      rel @ StoredPathRelation(_, _, _, _, fromRel, toRel, _, _), _, _) =>
        constrainedPerMatch(m).map(tuple => {
          val pathTuple: EdgeOrPathTuple = tuple.asInstanceOf[EdgeOrPathTuple]
          m.copy(
            relation = rel.copy(
              labelRelation = Relation(pathTuple.label),
              fromRel = fromRel.copy(labelRelation = Relation(pathTuple.fromLabel)),
              toRel = toRel.copy(labelRelation = Relation(pathTuple.toLabel))))
        })
    }
  }

  private def restrictLabelsPerMatch(relations: Seq[AlgebraTreeNode],
                                     constrainedLabels: BindingToLabelsMmap,
                                     graphDb: GraphDb): MatchToBindingTuplesMmap = {
    val matchToBindingTuplesMmap: MatchToBindingTuplesMmap = newMatchToBindingsMmap
    relations.foreach {
      case relation @ SimpleMatchRelation(VertexRelation(ref, _, _), _, _) =>
        constrainedLabels(ref).foreach(
          label => matchToBindingTuplesMmap.addBinding(relation, VertexTuple(label)))

      case relation @ SimpleMatchRelation(EdgeRelation(edgeRef, _, _, _, _), matchContext, _) =>
        val graphSchema: GraphSchema = matchContext.graph match {
          case DefaultGraph => graphDb.defaultGraph()
          case NamedGraph(graphName) => graphDb.graph(graphName)
        }
        val constrainedEdgeLabels: Seq[Label] = constrainedLabels(edgeRef).toSeq
        graphSchema.edgeRestrictions.map
          .filter(kv => constrainedEdgeLabels.contains(kv._1))
          .foreach(kv => {
            val edgeLabel: Label = kv._1
            val fromLabel: Label = kv._2._1
            val toLabel: Label = kv._2._2
            matchToBindingTuplesMmap
              .addBinding(relation, EdgeOrPathTuple(edgeLabel, fromLabel, toLabel))
          })

      case relation @ SimpleMatchRelation(
      StoredPathRelation(pathRef, _, _, _, _, _, _, _), matchContext, _) =>
        val graphSchema: GraphSchema = matchContext.graph match {
          case DefaultGraph => graphDb.defaultGraph()
          case NamedGraph(graphName) => graphDb.graph(graphName)
        }
        val constrainedPathLabels: Seq[Label] = constrainedLabels(pathRef).toSeq
        graphSchema.storedPathRestrictions.map
          .filter(kv => constrainedPathLabels.contains(kv._1))
          .foreach(kv => {
            val pathLabel: Label = kv._1
            val fromLabel: Label = kv._2._1
            val toLabel: Label = kv._2._2
            matchToBindingTuplesMmap
              .addBinding(relation, EdgeOrPathTuple(pathLabel, fromLabel, toLabel))
          })
    }

    matchToBindingTuplesMmap
  }

  private def restrictLabelsOverall(relations: Seq[SimpleMatchRelation],
                                    graphDb: GraphDb): BindingToLabelsMmap = {
    val initialRestrictedBindings: BindingToLabelsMmap = newBindingToLabelsMmap
    relations.foreach {
      case SimpleMatchRelation(VertexRelation(ref, _, _), matchContext, _) =>
        val graphSchema: GraphSchema = matchContext.graph match {
          case DefaultGraph => graphDb.defaultGraph()
          case NamedGraph(graphName) => graphDb.graph(graphName)
        }
        initialRestrictedBindings.update(ref, mutable.Set(graphSchema.vertexSchema.labels: _*))

      case SimpleMatchRelation(EdgeRelation(edgeRef, _, _, fromRel, toRel), matchContext, _) =>
        val graphSchema: GraphSchema = matchContext.graph match {
          case DefaultGraph => graphDb.defaultGraph()
          case NamedGraph(graphName) => graphDb.graph(graphName)
        }
        val vertexLabels: Seq[Label] = graphSchema.vertexSchema.labels
        initialRestrictedBindings.update(edgeRef, mutable.Set(graphSchema.edgeSchema.labels: _*))
        initialRestrictedBindings.update(fromRel.ref, mutable.Set(vertexLabels: _*))
        initialRestrictedBindings.update(toRel.ref, mutable.Set(vertexLabels: _*))

      case SimpleMatchRelation(
      StoredPathRelation(pathRef, _, _, _, fromRel, toRel, _, _), matchContext, _) =>
        val graphSchema: GraphSchema = matchContext.graph match {
          case DefaultGraph => graphDb.defaultGraph()
          case NamedGraph(graphName) => graphDb.graph(graphName)
        }
        val vertexLabels: Seq[Label] = graphSchema.vertexSchema.labels
        initialRestrictedBindings.update(pathRef, mutable.Set(graphSchema.pathSchema.labels: _*))
        initialRestrictedBindings.update(fromRel.ref, mutable.Set(vertexLabels: _*))
        initialRestrictedBindings.update(toRel.ref, mutable.Set(vertexLabels: _*))
    }

    analyseLabels(relations, graphDb, initialRestrictedBindings)
  }

  private def analyseLabels(relations: Seq[SimpleMatchRelation],
                            graphDb: GraphDb,
                            restrictedBindings: BindingToLabelsMmap): BindingToLabelsMmap = {
    var changed: Boolean = false
    relations.foreach {
      case SimpleMatchRelation(VertexRelation(ref, labelRelation, _), _, _) =>
        changed |= analyseSingleEndpRelation(ref, labelRelation, restrictedBindings)

      case SimpleMatchRelation(EdgeRelation(ref, edgeLblRel, _, fromRel, toRel), matchContext, _) =>
        val graphSchema: GraphSchema = matchContext.graph match {
          case DefaultGraph => graphDb.defaultGraph()
          case NamedGraph(graphName) => graphDb.graph(graphName)
        }
        changed |=
          analyseDoubleEndpRelation(
            ref, edgeLblRel, fromRel, toRel, graphSchema.edgeRestrictions, restrictedBindings)


      case SimpleMatchRelation(
      StoredPathRelation(ref, _, pathLblRel, _, fromRel, toRel, _, _), matchContext, _) =>
        val graphSchema: GraphSchema = matchContext.graph match {
          case DefaultGraph => graphDb.defaultGraph()
          case NamedGraph(graphName) => graphDb.graph(graphName)
        }
        changed |=
          analyseDoubleEndpRelation(
            ref, pathLblRel, fromRel, toRel, graphSchema.storedPathRestrictions, restrictedBindings)
    }

    if (changed)
      analyseLabels(relations, graphDb, restrictedBindings)
    else
      restrictedBindings
  }

  private def analyseSingleEndpRelation(ref: Reference,
                                        labelRelation: RelationLike,
                                        restrictedBindings: BindingToLabelsMmap): Boolean = {
    var changed: Boolean = false

    labelRelation match {
      case Relation(label) =>
        val currentBindings: mutable.Set[Label] = restrictedBindings(ref)
        val newBinding: mutable.Set[Label] = mutable.Set(label)
        if (!currentBindings.equals(newBinding)) {
          restrictedBindings.update(ref, newBinding)
          changed = true
        }
      case _ =>
    }

    changed
  }

  private def analyseDoubleEndpRelation(ref: Reference,
                                        labelRelation: RelationLike,
                                        fromRel: VertexRelation,
                                        toRel: VertexRelation,
                                        schemaRestrictions: SchemaMap[Label, (Label, Label)],
                                        restrictedBindings: BindingToLabelsMmap): Boolean = {
    var changed: Boolean = false

    fromRel.labelRelation match {
      // Try to update fromRel.ref binding depending on whether there is any label for fromRel.
      case Relation(label) =>
        val currentBindings: mutable.Set[Label] = restrictedBindings(fromRel.ref)
        val newBinding: mutable.Set[Label] = mutable.Set(label)
        if (!currentBindings.equals(newBinding)) {
          restrictedBindings.update(fromRel.ref, newBinding)
          changed = true
        }
      // Try to update fromRel.ref binding depending on the edge bindings.
      case AllRelations() =>
        val currentBindings: mutable.Set[Label] = restrictedBindings(fromRel.ref)
        val newBindings: mutable.Set[Label] =
          mutable.Set(
            schemaRestrictions.map
              // Extract edges that appear in the restrictions of current edge.
              .filter(kv => {
                val edgeOrPathLabel: Label = kv._1
                restrictedBindings(ref).contains(edgeOrPathLabel)
              })
              // Retain (from, to) tuple.
              .values
              // Retain left endpoint in (from, to) tuple (i.e, from).
              .map(_._1)
              .toSeq: _*)
        val intersection: mutable.Set[Label] = currentBindings intersect newBindings
        if (currentBindings.size != intersection.size) {
          restrictedBindings.update(fromRel.ref, intersection)
          changed = true
        }
    }

    labelRelation match {
      // Try to update this edge's bindings if there is a label assigned to it.
      case Relation(label) =>
        val currentBindings: mutable.Set[Label] = restrictedBindings(ref)
        val newBinding: mutable.Set[Label] = mutable.Set(label)
        if (!currentBindings.equals(newBinding)) {
          restrictedBindings.update(ref, newBinding)
          changed = true
        }
      // Try to update this edge's bindings based on the bindings of left and right endpoints.
      case AllRelations() =>
        val currentBindings: mutable.Set[Label] = restrictedBindings(ref)
        val newBindings: mutable.Set[Label] =
          mutable.Set(
            schemaRestrictions.map
              // Extract the edges that have the left endpoint in the left endpoint's
              // restrictions and the right endpoint in the right endpoint's restrictions.
              .filter(kv => {
                val fromLabel: Label = kv._2._1
                val toLabel: Label = kv._2._2
                restrictedBindings(fromRel.ref).contains(fromLabel) &&
                  restrictedBindings(toRel.ref).contains(toLabel)
              })
              .keys
              .toSeq: _*)
        val intersection: mutable.Set[Label] = currentBindings intersect newBindings
        if (currentBindings.size != intersection.size) {
          restrictedBindings.update(ref, intersection)
          changed = true
        }
    }

    toRel.labelRelation match {
      // Try to update toRel.ref binding depending on whether there is any label for toRel.
      case Relation(label) =>
        val currentBindings: mutable.Set[Label] = restrictedBindings(toRel.ref)
        val newBinding: mutable.Set[Label] = mutable.Set(label)
        if (!currentBindings.equals(newBinding)) {
          restrictedBindings.update(toRel.ref, newBinding)
          changed = true
        }
      // Try to update toRel.ref binding depending on the edge bindings.
      case AllRelations() =>
        val currentBindings: mutable.Set[Label] = restrictedBindings(toRel.ref)
        val newBindings: mutable.Set[Label] =
          mutable.Set(
            schemaRestrictions.map
              // Extract edges that appear in the restrictions of current edge.
              .filter(kv => {
                val edgeOrPathLabel: Label = kv._1
                restrictedBindings(ref).contains(edgeOrPathLabel)
              })
              // Retain (from, to) tuple.
              .values
              // Retain right endpoint in (from, to) tuple.
              .map(_._2)
              .toSeq: _*)
        val intersection: mutable.Set[Label] = currentBindings intersect newBindings
        if (currentBindings.size != intersection.size) {
          restrictedBindings.update(toRel.ref, intersection)
          changed = true
        }
    }

    changed
  }
}
