package algebra.trees

import algebra.expressions.{Label, Reference}
import algebra.operators._
import algebra.types.{DefaultGraph, Graph, NamedGraph}
import common.trees.TopDownRewriter
import schema.{GraphDb, GraphSchema, SchemaMap}

import scala.collection.mutable

case class ExpandRelations(context: AlgebraContext) extends TopDownRewriter[AlgebraTreeNode] {

  sealed abstract class EntityTuple
  sealed case class VertexTuple(label: Label) extends EntityTuple
  sealed case class EdgeOrPathTuple(edgeLabel: Label, fromLabel: Label, toLabel: Label)
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
    case condMatchClause @ CondMatchClause(_, matchPred) =>
      val graphDb: GraphDb = context.graphDb
      val simpleMatches: Seq[SimpleMatchRelation] =
        condMatchClause.children.init.flatMap(m => {
          val graphPattern: AlgebraTreeNode = m.children.head
          graphPattern.children.map(pattern =>
            SimpleMatchRelation(
              relation = pattern.asInstanceOf[RelationLike],
              context = SimpleMatchRelationContext(m.children.last.asInstanceOf[Graph])
            ))
        })

      val constrainedLabels: BindingToLabelsMmap = restrictLabelsOverall(simpleMatches, graphDb)
      val constrainedPerMatch: MatchToBindingTuplesMmap =
        restrictLabelsPerMatch(simpleMatches, constrainedLabels, graphDb)
      val expandedSimpleMatches: Seq[SimpleMatchRelation] =
        simpleMatches.flatMap {
          case m @ SimpleMatchRelation(rel @ VertexRelation(_, _, _), _, _) =>
            constrainedPerMatch(m).map(tuple =>
              m.copy(
                relation = rel.copy(labelRelation = Relation(tuple.asInstanceOf[VertexTuple].label))
              )
            )
          case m @ SimpleMatchRelation(rel @ EdgeRelation(_, _, _, fromRel, toRel), _, _) =>
            constrainedPerMatch(m).map(tuple => {
              val edgeTuple: EdgeOrPathTuple = tuple.asInstanceOf[EdgeOrPathTuple]
              m.copy(
                relation = rel.copy(
                  labelRelation = Relation(edgeTuple.edgeLabel),
                  fromRel = fromRel.copy(labelRelation = Relation(edgeTuple.fromLabel)),
                  toRel = toRel.copy(labelRelation = Relation(edgeTuple.toLabel))))
            })
          case m @ SimpleMatchRelation(
          rel @ StoredPathRelation(_, _, _, _, fromRel, toRel, _, _), _, _) =>
            constrainedPerMatch(m).map(tuple => {
              val pathTuple: EdgeOrPathTuple = tuple.asInstanceOf[EdgeOrPathTuple]
              m.copy(
                relation = rel.copy(
                  labelRelation = Relation(pathTuple.edgeLabel),
                  fromRel = fromRel.copy(labelRelation = Relation(pathTuple.fromLabel)),
                  toRel = toRel.copy(labelRelation = Relation(pathTuple.toLabel))))
            })
        }

      condMatchClause.children = expandedSimpleMatches ++ Seq(matchPred)
      condMatchClause
  }

  private def restrictLabelsPerMatch(relations: Seq[SimpleMatchRelation],
                                     constrainedLabels: BindingToLabelsMmap,
                                     graphDb: GraphDb): MatchToBindingTuplesMmap = {
    val matchToBindingTuplesMmap: MatchToBindingTuplesMmap = newMatchToBindingsMmap
    relations.foreach {
      case relation @ SimpleMatchRelation(VertexRelation(ref, _, _), _, _) =>
        constrainedLabels(ref).foreach(
          label => matchToBindingTuplesMmap.addBinding(relation, VertexTuple(label)))

      case relation @ SimpleMatchRelation(EdgeRelation(edgeRef, _, _, _, _), matchContext, _) =>
        val graphSchema: GraphSchema = matchContext.graph match {
          case DefaultGraph() => graphDb.defaultGraph()
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
          case DefaultGraph() => graphDb.defaultGraph()
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
          case DefaultGraph() => graphDb.defaultGraph()
          case NamedGraph(graphName) => graphDb.graph(graphName)
        }
        initialRestrictedBindings.update(ref, mutable.Set(graphSchema.vertexSchema.labels: _*))

      case SimpleMatchRelation(EdgeRelation(edgeRef, _, _, fromRel, toRel), matchContext, _) =>
        val graphSchema: GraphSchema = matchContext.graph match {
          case DefaultGraph() => graphDb.defaultGraph()
          case NamedGraph(graphName) => graphDb.graph(graphName)
        }
        val vertexLabels: Seq[Label] = graphSchema.vertexSchema.labels
        initialRestrictedBindings.update(edgeRef, mutable.Set(graphSchema.edgeSchema.labels: _*))
        initialRestrictedBindings.update(fromRel.ref, mutable.Set(vertexLabels: _*))
        initialRestrictedBindings.update(toRel.ref, mutable.Set(vertexLabels: _*))

      case SimpleMatchRelation(
      StoredPathRelation(pathRef, _, _, _, fromRel, toRel, _, _), matchContext, _) =>
        val graphSchema: GraphSchema = matchContext.graph match {
          case DefaultGraph() => graphDb.defaultGraph()
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
        analyseSingleEndpRelation(ref, labelRelation, restrictedBindings)

      case SimpleMatchRelation(EdgeRelation(ref, edgeLblRel, _, fromRel, toRel), matchContext, _) =>
        val graphSchema: GraphSchema = matchContext.graph match {
          case DefaultGraph() => graphDb.defaultGraph()
          case NamedGraph(graphName) => graphDb.graph(graphName)
        }
        changed =
          analyseDoubleEndpRelation(
            ref, edgeLblRel, fromRel, toRel, graphSchema.edgeRestrictions, restrictedBindings)


      case SimpleMatchRelation(
      StoredPathRelation(ref, _, pathLblRel, _, fromRel, toRel, _, _), matchContext, _) =>
        val graphSchema: GraphSchema = matchContext.graph match {
          case DefaultGraph() => graphDb.defaultGraph()
          case NamedGraph(graphName) => graphDb.graph(graphName)
        }
        changed =
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
              .filter(edgeToFromAnToTuple => {
                val edgeLabel: Label = edgeToFromAnToTuple._1
                restrictedBindings(ref).contains(edgeLabel)
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
              .filter(edgeToFromAndToTuple => {
                val fromLabel: Label = edgeToFromAndToTuple._2._1
                val toLabel: Label = edgeToFromAndToTuple._2._2
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
              .filter(edgeToFromAnToTuple => {
                val edgeLabel: Label = edgeToFromAnToTuple._1
                restrictedBindings(ref).contains(edgeLabel)
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
