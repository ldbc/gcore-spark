package algebra.trees

import algebra.expressions.{DisjunctLabels, Exists, Label, Reference}
import algebra.operators._
import algebra.types._
import common.exceptions.UnsupportedOperation
import common.trees.TopDownRewriter
import schema.EntitySchema.LabelRestrictionMap
import schema.{Catalog, GraphSchema, SchemaMap}

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
  * ([[VertexRelation]], [[EdgeRelation]], [[StoredPathRelation]], [[VirtualPathRelation]]) have
  * been either labeled with a fixed label ([[Relation]]), if this was provided in the query, or
  * with the label [[AllRelations]], which means that at that point in the rewrite pipeline, we
  * could only infer that the data for the respective variable is to be queried in all available
  * tables for its type.
  *
  * During the inference process, each [[Connection]] in a [[GraphPattern]] becomes a
  * [[SimpleMatchRelation]]. At the end of the inference process:
  * - if a [[Connection]] is strictly labeled (there is only one [[Label]] in the minimal set), then
  * we emit a single [[SimpleMatchRelation]] for that [[Connection]];
  * - otherwise, we emit as many [[SimpleMatchRelation]]s as the size of the minimal set and
  * preserve the binding tuple of the [[Connection]] in the newly formed relations.
  */
case class ExpandRelations(context: AlgebraContext) extends TopDownRewriter[AlgebraTreeNode] {

  sealed abstract class LabelTuple
  sealed case class VertexTuple(label: Label) extends LabelTuple
  sealed case class EdgeOrPathTuple(label: Label, fromLabel: Label, toLabel: Label)
    extends LabelTuple
  sealed case class VirtualPathTuple(fromLabel: Label, toLabel: Label) extends LabelTuple

  private type BindingToLabelsMmap = mutable.HashMap[Reference, mutable.Set[Label]]
    with mutable.MultiMap[Reference, Label]

  private type MatchToBindingTuplesMmap =
    mutable.HashMap[SimpleMatchRelation, mutable.Set[LabelTuple]]
      with mutable.MultiMap[SimpleMatchRelation, LabelTuple]

  private def newBindingToLabelsMmap: BindingToLabelsMmap =
    new mutable.HashMap[Reference, mutable.Set[Label]]
      with mutable.MultiMap[Reference, Label]

  private def newMatchToBindingsMmap: MatchToBindingTuplesMmap =
    new mutable.HashMap[SimpleMatchRelation, mutable.Set[LabelTuple]]
      with mutable.MultiMap[SimpleMatchRelation, LabelTuple]

  override val rule: RewriteFuncType = {
    case matchClause: MatchClause =>
      val catalog: Catalog = context.catalog

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
      val constrainedLabels: BindingToLabelsMmap = restrictLabelsOverall(allSimpleMatches, catalog)

      // Constrain labels for each CondMatchClause.
      matchClause.children.foreach(
        condMatch => {
          val simpleMatches: Seq[SimpleMatchRelation] =
            constrainSimpleMatches(
              simpleMatches = condMatch.children.init, constrainedLabels, catalog)
          val matchPred: AlgebraTreeNode = condMatch.children.last
          condMatch.children = simpleMatches :+ matchPred

          // If the matching predicate is an existence clause, we also constrain the simple matches
          // under the Exists condition.
          matchPred forEachDown {
            case e: Exists =>
              val existsMatches: Seq[SimpleMatchRelation] =
                constrainSimpleMatches(simpleMatches = e.children, constrainedLabels, catalog)
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

  /**
    * Given a:
    *   - sequence of [[SimpleMatchRelation]]s
    *   - a mapping from each [[SimpleMatchRelation]] to a sequence of [[LabelTuple]]s,
    *
    * emit for each [[SimpleMatchRelation]] as many equivalent relations as there are label tuples
    * mapped to it.
    */
  private def constrainSimpleMatches(simpleMatches: Seq[AlgebraTreeNode],
                                     constrainedLabels: BindingToLabelsMmap,
                                     catalog: Catalog): Seq[SimpleMatchRelation] = {
    val constrainedPerMatch: MatchToBindingTuplesMmap =
      restrictLabelsPerMatch(simpleMatches, constrainedLabels, catalog)

    simpleMatches.flatMap {
      case m @ SimpleMatchRelation(rel: VertexRelation, _, _) =>
        constrainedPerMatch(m).collect {
          case VertexTuple(label) => m.copy(relation = rel.copy(labelRelation = Relation(label)))
          case other =>
            throw new IllegalArgumentException(
              s"A match relation of vertex should not be mapped to $other")
        }
      case m @ SimpleMatchRelation(rel: EdgeRelation, _, _) =>
        constrainedPerMatch(m).collect {
          case EdgeOrPathTuple(label, fromLabel, toLabel) =>
            m.copy(
              relation = rel.copy(
                labelRelation = Relation(label),
                fromRel = rel.fromRel.copy(labelRelation = Relation(fromLabel)),
                toRel = rel.toRel.copy(labelRelation = Relation(toLabel))))
          case other =>
            throw new IllegalArgumentException(
              s"A match relation of edge should not be mapped to $other")
        }
      case m @ SimpleMatchRelation(rel: StoredPathRelation, _, _) =>
        constrainedPerMatch(m).collect {
          case EdgeOrPathTuple(label, fromLabel, toLabel) =>
            m.copy(
              relation = rel.copy(
                labelRelation = Relation(label),
                fromRel = rel.fromRel.copy(labelRelation = Relation(fromLabel)),
                toRel = rel.toRel.copy(labelRelation = Relation(toLabel))))
          case other =>
            throw new IllegalArgumentException(
              s"A match relation of stored path should not be mapped to $other")
        }
      case m @ SimpleMatchRelation(rel: VirtualPathRelation, _, _) =>
        constrainedPerMatch(m).collect {
          case VirtualPathTuple(fromLabel, toLabel) =>
            m.copy(
              relation = rel.copy(
                fromRel = rel.fromRel.copy(labelRelation = Relation(fromLabel)),
                toRel = rel.toRel.copy(labelRelation = Relation(toLabel))))
          case other =>
            throw new IllegalArgumentException(
              s"A match relation of virtual path should not be mapped to $other")
        }
    }
  }

  /**
    * Given a:
    *   - sequence of [[SimpleMatchRelation]]s
    *   - a mapping between each [[Reference]] that appears in those relations and a set of labels
    *   that can be applied to that [[Reference]],
    *
    * create a multi-mapping between each [[SimpleMatchRelation]] in the sequence and the
    * [[LabelTuple]]s that can be applied to the entire relation.
    *
    * For example, if we had the following reference-to-label mapping:
    *   a -> [A1, A2]
    *   b -> [B1, B2]
    *   e -> [E1, E2], where E1 can only occur between A1-B1 and E2 only between A2-B2,
    *
    * then we would map the relation (a)-[e]->(b) to: (A1, E1, B1), (A2, E2, B2).
    */
  private def restrictLabelsPerMatch(relations: Seq[AlgebraTreeNode],
                                     constrainedLabels: BindingToLabelsMmap,
                                     catalog: Catalog): MatchToBindingTuplesMmap = {
    val matchToBindingTuplesMmap: MatchToBindingTuplesMmap = newMatchToBindingsMmap
    relations.foreach {
      case relation @ SimpleMatchRelation(vertex: VertexRelation, _, _) =>
        constrainedLabels(vertex.ref).foreach(
          label => matchToBindingTuplesMmap.addBinding(relation, VertexTuple(label)))

      case relation @ SimpleMatchRelation(edge: EdgeRelation, matchContext, _) =>
        val graphSchema: GraphSchema = extractGraphSchema(matchContext, catalog)
        val constrainedEdgeLabels: Seq[Label] = constrainedLabels(edge.ref).toSeq
        graphSchema.edgeRestrictions.map
          .filter { case (edgeLabel, _) => constrainedEdgeLabels.contains(edgeLabel) }
          .foreach {
            case (edgeLabel, (fromLabel, toLabel)) =>
              matchToBindingTuplesMmap.addBinding(
                relation, EdgeOrPathTuple(edgeLabel, fromLabel, toLabel))
          }

      case relation @ SimpleMatchRelation(path: StoredPathRelation, matchContext, _) =>
        val graphSchema: GraphSchema = extractGraphSchema(matchContext, catalog)
        val constrainedPathLabels: Seq[Label] = constrainedLabels(path.ref).toSeq
        graphSchema.storedPathRestrictions.map
          .filter { case (pathLabel, _) => constrainedPathLabels.contains(pathLabel) }
          .foreach {
            case (pathLabel, (fromLabel, toLabel)) =>
              matchToBindingTuplesMmap.addBinding(
                relation, EdgeOrPathTuple(pathLabel, fromLabel, toLabel))
          }

      case relation @ SimpleMatchRelation(path: VirtualPathRelation, _, _) =>
        val fromLabels: Seq[Label] = constrainedLabels(path.fromRel.ref).toSeq
        val toLabels: Seq[Label] = constrainedLabels(path.toRel.ref).toSeq
        val labelsCrossProd: Seq[(Label, Label)] =
          for { fromLabel <- fromLabels; toLabel <- toLabels } yield (fromLabel, toLabel)
        labelsCrossProd.foreach {
          case (fromLabel, toLabel) =>
            matchToBindingTuplesMmap.addBinding(relation, VirtualPathTuple(fromLabel, toLabel))
        }
    }

    matchToBindingTuplesMmap
  }

  /**
    * Given a sequence of [[SimpleMatchRelation]]s, creates a multi-mapping between each
    * [[Reference]] that appears in the sequence of relations and labels that can be applied to
    * that [[Reference]], in the overall context of the relations.
    */
  private def restrictLabelsOverall(relations: Seq[SimpleMatchRelation],
                                    catalog: Catalog): BindingToLabelsMmap = {
    val initialRestrictedBindings: BindingToLabelsMmap = newBindingToLabelsMmap
    relations.foreach {
      case SimpleMatchRelation(VertexRelation(ref, _, _), matchContext, _) =>
        val graphSchema: GraphSchema = extractGraphSchema(matchContext, catalog)
        initialRestrictedBindings.update(ref, mutable.Set(graphSchema.vertexSchema.labels: _*))

      case SimpleMatchRelation(EdgeRelation(edgeRef, _, _, fromRel, toRel), matchContext, _) =>
        val graphSchema: GraphSchema = extractGraphSchema(matchContext, catalog)
        val vertexLabels: Seq[Label] = graphSchema.vertexSchema.labels
        initialRestrictedBindings.update(edgeRef, mutable.Set(graphSchema.edgeSchema.labels: _*))
        initialRestrictedBindings.update(fromRel.ref, mutable.Set(vertexLabels: _*))
        initialRestrictedBindings.update(toRel.ref, mutable.Set(vertexLabels: _*))

      case SimpleMatchRelation(
      VirtualPathRelation(ref, _, fromRel, toRel, _, _), matchContext, _) =>
        val graphSchema: GraphSchema = extractGraphSchema(matchContext, catalog)
        val vertexLabels: Seq[Label] = graphSchema.vertexSchema.labels
        initialRestrictedBindings.update(
          vpathFromEdgeReference(ref), mutable.Set(graphSchema.edgeSchema.labels: _*))
        initialRestrictedBindings.update(
          vpathToEdgeReference(ref), mutable.Set(graphSchema.edgeSchema.labels: _*))
        initialRestrictedBindings.update(fromRel.ref, mutable.Set(vertexLabels: _*))
        initialRestrictedBindings.update(toRel.ref, mutable.Set(vertexLabels: _*))

      case SimpleMatchRelation(
      StoredPathRelation(pathRef, _, _, _, fromRel, toRel, _, _), matchContext, _) =>
        val graphSchema: GraphSchema = extractGraphSchema(matchContext, catalog)
        val vertexLabels: Seq[Label] = graphSchema.vertexSchema.labels
        initialRestrictedBindings.update(pathRef, mutable.Set(graphSchema.pathSchema.labels: _*))
        initialRestrictedBindings.update(fromRel.ref, mutable.Set(vertexLabels: _*))
        initialRestrictedBindings.update(toRel.ref, mutable.Set(vertexLabels: _*))
    }

    analyseLabels(relations, catalog, initialRestrictedBindings)
  }

  private def analyseLabels(relations: Seq[SimpleMatchRelation],
                            catalog: Catalog,
                            restrictedBindings: BindingToLabelsMmap): BindingToLabelsMmap = {
    var changed: Boolean = false
    relations.foreach {
      case SimpleMatchRelation(VertexRelation(ref, labelRelation, _), _, _) =>
        changed |= analyseVertexRelation(ref, labelRelation, restrictedBindings)

      case SimpleMatchRelation(EdgeRelation(ref, edgeLblRel, _, fromRel, toRel), matchContext, _) =>
        val graphSchema: GraphSchema = extractGraphSchema(matchContext, catalog)
        changed |=
          analyseEdgeOrStoredPath(
            ref, edgeLblRel, fromRel, toRel, graphSchema.edgeRestrictions, restrictedBindings)

      case SimpleMatchRelation(
      StoredPathRelation(ref, _, pathLblRel, _, fromRel, toRel, _, _), matchContext, _) =>
        val graphSchema: GraphSchema = extractGraphSchema(matchContext, catalog)
        changed |=
          analyseEdgeOrStoredPath(
            ref, pathLblRel, fromRel, toRel, graphSchema.storedPathRestrictions, restrictedBindings)

      case SimpleMatchRelation(
      VirtualPathRelation(ref, _, fromRel, toRel, _, pathExpr), matchContext, _) =>
        val graphSchema: GraphSchema = extractGraphSchema(matchContext, catalog)
        changed |=
          analyseVirtualPath(
            ref, fromRel, toRel, pathExpr, graphSchema.edgeRestrictions, restrictedBindings)
    }

    if (changed)
      analyseLabels(relations, catalog, restrictedBindings)
    else
      restrictedBindings
  }

  private def analyseVertexRelation(ref: Reference,
                                    labelRelation: RelationLike,
                                    restrictedBindings: BindingToLabelsMmap): Boolean = {
    var changed: Boolean = false
    labelRelation match {
      case Relation(label) => changed = tryUpdateStrictLabel(ref, label, restrictedBindings)
      case _ =>
    }
    changed
  }

  private def analyseVirtualPath(reference: Reference,
                                 fromRel: VertexRelation,
                                 toRel: VertexRelation,
                                 pathExpression: Option[PathExpression],
                                 schemaRestrictions: LabelRestrictionMap,
                                 restrictedBindings: BindingToLabelsMmap): Boolean = {
    pathExpression match {
      case Some(KleeneStar(DisjunctLabels(labels), _, _)) =>
        val fromEdgeLabel: Label = labels.head
        val toEdgeLabel: Label = labels.last
        val fromEdgeReference: Reference = vpathFromEdgeReference(reference)
        val toEdgeReference: Reference = vpathToEdgeReference(reference)

        val fromLabelUpdated: Boolean =
          tryUpdateFromLabel(fromEdgeReference, fromRel, schemaRestrictions, restrictedBindings)
        val fromEdgeLabelUpdated: Boolean =
          tryUpdateStrictLabel(fromEdgeReference, fromEdgeLabel, restrictedBindings)
        val toEdgeLabelUpdated: Boolean =
          tryUpdateStrictLabel(toEdgeReference, toEdgeLabel, restrictedBindings)
        val toLabelUpdated: Boolean =
          tryUpdateToLabel(toEdgeReference, toRel, schemaRestrictions, restrictedBindings)

        fromLabelUpdated | fromEdgeLabelUpdated | toEdgeLabelUpdated | toLabelUpdated

      case _ =>
        throw UnsupportedOperation("Unsupported path configuration.")
    }
  }

  private def analyseEdgeOrStoredPath(ref: Reference,
                                      labelRelation: RelationLike,
                                      fromRel: VertexRelation,
                                      toRel: VertexRelation,
                                      schemaRestrictions: LabelRestrictionMap,
                                      restrictedBindings: BindingToLabelsMmap): Boolean = {
    val fromLabelUpdated: Boolean =
      tryUpdateFromLabel(ref, fromRel, schemaRestrictions, restrictedBindings)
    val edgeOrPathLabelUpdated: Boolean =
      tryUpdateEdgeOrPathLabel(
        ref, labelRelation, fromRel, toRel, schemaRestrictions, restrictedBindings)
    val toLabelUpdated: Boolean =
      tryUpdateToLabel(ref, toRel, schemaRestrictions, restrictedBindings)

    fromLabelUpdated | edgeOrPathLabelUpdated | toLabelUpdated
  }

  private def tryUpdateStrictLabel(reference: Reference,
                                   label: Label,
                                   restrictedBindings: BindingToLabelsMmap): Boolean = {
    var changed: Boolean = false
    val currentBindings: mutable.Set[Label] = restrictedBindings(reference)
    val newBinding: mutable.Set[Label] = mutable.Set(label)
    if (!currentBindings.equals(newBinding)) {
      restrictedBindings.update(reference, newBinding)
      changed = true
    }

    changed
  }

  private def tryUpdateFromLabel(edgeReference: Reference,
                                 fromRel: VertexRelation,
                                 schemaRestrictions: LabelRestrictionMap,
                                 restrictedBindings: BindingToLabelsMmap): Boolean = {
    var changed: Boolean = false
    fromRel.labelRelation match {
      // Try to update fromRel.ref binding depending on whether there is any label for fromRel.
      case Relation(label) => changed = tryUpdateStrictLabel(fromRel.ref, label, restrictedBindings)
      // Try to update fromRel.ref binding depending on the edge bindings.
      case AllRelations =>
        val currentBindings: mutable.Set[Label] = restrictedBindings(fromRel.ref)
        val newBindings: mutable.Set[Label] =
          mutable.Set(
            schemaRestrictions.map
              .filter {
                case (edgeOrPathLabel, _) =>
                  restrictedBindings(edgeReference).contains(edgeOrPathLabel)
              }
              .values // (from, to) label tuples
              .map(_._1) // take left endpoint of tuple (from)
              .toSeq: _*)
        val intersection: mutable.Set[Label] = currentBindings intersect newBindings
        if (currentBindings.size != intersection.size) {
          restrictedBindings.update(fromRel.ref, intersection)
          changed = true
        }
    }

    changed
  }

  private def tryUpdateToLabel(edgeReference: Reference,
                               toRel: VertexRelation,
                               schemaRestrictions: LabelRestrictionMap,
                               restrictedBindings: BindingToLabelsMmap): Boolean = {
    var changed: Boolean = false
    toRel.labelRelation match {
      // Try to update toRel.ref binding depending on whether there is any label for toRel.
      case Relation(label) => changed = tryUpdateStrictLabel(toRel.ref, label, restrictedBindings)
      // Try to update toRel.ref binding depending on the edge bindings.
      case AllRelations =>
        val currentBindings: mutable.Set[Label] = restrictedBindings(toRel.ref)
        val newBindings: mutable.Set[Label] =
          mutable.Set(
            schemaRestrictions.map
              .filter {
                case (edgeOrPathLabel, _) =>
                  restrictedBindings(edgeReference).contains(edgeOrPathLabel)
              }
              .values // (from, to) label tuples
              .map(_._2) // take right endpoint of tuple (to)
              .toSeq: _*)
        val intersection: mutable.Set[Label] = currentBindings intersect newBindings
        if (currentBindings.size != intersection.size) {
          restrictedBindings.update(toRel.ref, intersection)
          changed = true
        }
    }
    changed
  }

  private def tryUpdateEdgeOrPathLabel(ref: Reference,
                                       labelRelation: RelationLike,
                                       fromRel: VertexRelation,
                                       toRel: VertexRelation,
                                       schemaRestrictions: LabelRestrictionMap,
                                       restrictedBindings: BindingToLabelsMmap): Boolean = {
    var changed: Boolean = false
    labelRelation match {
      // Try to update this edge's bindings if there is a label assigned to it.
      case Relation(label) => changed = tryUpdateStrictLabel(ref, label, restrictedBindings)
      // Try to update this edge's bindings based on the bindings of left and right endpoints.
      case AllRelations =>
        val currentBindings: mutable.Set[Label] = restrictedBindings(ref)
        val newBindings: mutable.Set[Label] =
          mutable.Set(
            schemaRestrictions.map
              // Extract the edges that have the left endpoint in the left endpoint's
              // restrictions and the right endpoint in the right endpoint's restrictions.
              .filter {
              case (_, (fromLabel, toLabel)) =>
                restrictedBindings(fromRel.ref).contains(fromLabel) &&
                  restrictedBindings(toRel.ref).contains(toLabel)
            }
              .keys
              .toSeq: _*)
        val intersection: mutable.Set[Label] = currentBindings intersect newBindings
        if (currentBindings.size != intersection.size) {
          restrictedBindings.update(ref, intersection)
          changed = true
        }
    }
    changed
  }

  private def extractGraphSchema(matchContext: SimpleMatchRelationContext,
                                 catalog: Catalog): GraphSchema = {
    matchContext.graph match {
      case DefaultGraph => catalog.defaultGraph()
      case NamedGraph(graphName) => catalog.graph(graphName)
    }
  }

  private def vpathFromEdgeReference(pathRef: Reference): Reference =
    Reference(s"${pathRef}_from")

  private def vpathToEdgeReference(pathRef: Reference): Reference =
    Reference(s"${pathRef}_to")

}
