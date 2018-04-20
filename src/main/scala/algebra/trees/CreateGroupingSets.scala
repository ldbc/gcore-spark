package algebra.trees

import algebra.expressions._
import algebra.operators._
import algebra.trees.CreateGroupingSets._
import algebra.types.{ConstructPattern, GroupDeclaration, VertexConstruct}
import common.trees.BottomUpRewriter
import parser.utils.VarBinder.createVar

import scala.collection.mutable

object CreateGroupingSets {

  /** Basename of aggregates that participate in a SET clause or inline property set. */
  val PROP_AGG_BASENAME: String = "propset_agg"

  def processBindingTable(reference: Reference,
                          bindingTable: RelationLike,
                          when: AlgebraExpression,
                          groupingAttributes: Seq[AlgebraTreeNode],
                          aggregateFunctions: Seq[PropertySet],
                          projectAttributes: Set[Reference]): RelationLike = {
    Project(
      GroupBy(
        reference,
        relation = Select(bindingTable, expr = when),
        groupingAttributes,
        aggregateFunctions),
      attributes = projectAttributes)
  }
}

/**
  * Rewrites the CONSTRUCT sub-tree, such that each individual [[CondConstructClause]] becomes its
  * own group construct. TODO: Add reference to the actual class here, once implemented.
  *
  * The construction process begins with the [[BindingTable]], which is first filtered according to
  * the WHEN sub-clause of the [[BasicConstructClause]].
  *
  * An entity can be built from a variable that has been bound in the MATCH sub-clause so is now
  * present in the binding table, or from an unbound variable, that has no equivalent in the
  * binding table. Variables present in the binding table that are used in the CONSTRUCT clause need
  * to stick to their original identity.
  *
  * --- Building a vertex ---
  * For a vertex built from a matched variable, the filtered binding table is <i>grouped by</i> that
  * vertex's identity. No other grouping is allowed in this case, as it would violate the variable's
  * identity, by replacing properties with their aggregates.
  *
  * An unbound vertex can use a specific [[GroupDeclaration]] to coalesce certain properties of the
  * binding table. However, if no GROUP-ing is specified in this case, then no grouping attributes
  * will be used to group the binding table.
  *
  * New properties and labels can be added or removed from the vertex. The [[SetClause]] and the
  * [[RemoveClause]] of the built vertex will contain only those properties or labels that have
  * been specified for that vertex. For example, if we build (c) and (f) and SET prop_c for vertex
  * (c) and prop_f for vertex (f), then only prop_c will be passed to c's
  * [[VertexConstructRelation]] and only prop_f will be passed to f's [[VertexConstructRelation]].
  *
  * New properties can be the result of an [[AggregateExpression]]. In this case, an
  * [[AggregateExpression]] (or, more specifically, the entire [[AlgebraExpression]] sub-tree that
  * contains that aggregation) is substituted with a new property assigned to the vertex and
  * used as a [[GroupBy.aggregateFunctions]] parameter. For example, for the following pattern:
  *
  * > CONSTRUCT (a { avg_prop := AVG(b.prop) }) SET a.count_prop := COUNT(*)
  *
  * We substitute avg_prop and count_prop with the reference at two new properties for a:
  *    a.avg_prop   := a.propset_agg_xy
  *    a.count_prop := a.propset_agg_zt
  *
  * and then pass to the [[GroupBy]] as its [[GroupBy.aggregateFunctions]] the [[PropertySet]]s:
  *    a.propset_agg_xy := AVG(b.prop)
  *    a.propset_agg_zt := COUNT(*)
  *
  * Then the grouped binding table produced by [[GroupBy]] will contain the two new aggregated
  * properties, propset_agg_xy and propset_agg_zt which will then be used to create the vertex (a).
  * As these two properties are only used as temporary results, they will be added to a's
  * [[RemoveClause]].
  *
  * Finally, from the aggregated binding table we <i>project</i> the vertex plus any additional
  * variable that will be used in a new property. The vertex is then built using through a
  * [[VertexConstructRelation]].
  */
case class CreateGroupingSets(context: AlgebraContext) extends BottomUpRewriter[AlgebraTreeNode] {

  assert(context.bindingContext.isDefined,
    "The bindingContext in AlgebraContext needs to be defined for this rewrite stage.")

  private val bindingContext: BindingContext = context.bindingContext.get
  private val bindingTable: BindingTable = BindingTable(BindingSet(bindingContext.allRefs))

  private val constructClause: RewriteFuncType = {
    case construct: ConstructClause =>
      val setClause: SetClause = construct.children(2).asInstanceOf[SetClause]
      val removeClause: RemoveClause = construct.children.last.asInstanceOf[RemoveClause]
      val bindingToSetClause: Map[Reference, SetClause] = mapSetClauseToBinding(setClause)
      val bindingToRemoveClause: Map[Reference, RemoveClause] =
        mapRemoveClauseToBinding(removeClause)

      val condConstruct: CondConstructClause =
        construct.children(1).asInstanceOf[CondConstructClause]
      val basicConstructs: Seq[AlgebraTreeNode] = condConstruct.children
      val constructRelations: Seq[RelationLike] =
        basicConstructs.map(createGroupConstruct(_, bindingToSetClause, bindingToRemoveClause))

      condConstruct.children = constructRelations
      construct.children = List(construct.graphs, condConstruct)
      construct
  }

  override val rule: RewriteFuncType = constructClause

  private
  def createGroupConstruct(basicConstruct: AlgebraTreeNode,
                           bindingToSetClause: Map[Reference, SetClause],
                           bindingToRemoveClause: Map[Reference, RemoveClause]): RelationLike = {

    val constructPattern: ConstructPattern =
      basicConstruct.children.head.asInstanceOf[ConstructPattern]
    val when: AlgebraExpression = basicConstruct.children.last.asInstanceOf[AlgebraExpression]

    val vertex: VertexConstruct = constructPattern.children.head.asInstanceOf[VertexConstruct]

    singleEndpointRelation(
      vertex, when,
      bindingToSetClause.get(vertex.ref), bindingToRemoveClause.get(vertex.ref))
  }

  private
  def singleEndpointRelation(vertex: VertexConstruct,
                             when: AlgebraExpression,
                             setClause: Option[SetClause],
                             removeClause: Option[RemoveClause]): RelationLike = {

    // If the vertex construct reference was already matched, then we only need to create a
    // grouping of the binding table for that specific reference. Otherwise, we are creating a
    // vertex from an unmatched binding.
    val isMatchRef: Boolean = bindingContext.allRefs.contains(vertex.ref)

    // For inline property assignments, pull out aggregations and replace with new property for this
    // vertex.
    val normalizePropAssignmentsRes: (PropAssignments, Seq[PropertySet]) =
      normalizePropAssignments(
        vertex.ref, vertex.expr.propAssignments.asInstanceOf[PropAssignments])
    // For SET property assignments, pull out aggregations and replace with new property for this
    // vertex.
    val normalizeSetClauseRes: Option[(SetClause, Seq[PropertySet])] = {
      if (setClause.isDefined)
        Some(normalizeSetClause(vertex.ref, setClause.get))
      else
        None
    }
    val newPropAssignments: PropAssignments = normalizePropAssignmentsRes._1
    val newSetClause: Option[SetClause] = {
      if (normalizeSetClauseRes.isDefined)
        Some(normalizeSetClauseRes.get._1)
      else
        None
    }
    val propAggregates: Seq[PropertySet] =
      normalizePropAssignmentsRes._2 ++ {
        if (normalizeSetClauseRes.isDefined)
          normalizeSetClauseRes.get._2
        else
          Seq.empty
      }

    // If we added any new properties to replace the aggregates, we need to remove them from the
    // final result.
    val removeClauseWithPropAggregates: Option[RemoveClause] = {
      val removePropAggregates: Seq[PropertyRemove] =
        propAggregates.map(propertySet =>
          PropertyRemove(PropertyRef(propertySet.ref, propertySet.propAssignment.propKey)))

      if (removeClause.isDefined)
        Some(
          removeClause.get
            .copy(propRemoves = removeClause.get.propRemoves ++ removePropAggregates))
      else if (removePropAggregates.nonEmpty)
        Some(
          RemoveClause(
            propRemoves = removePropAggregates,
            labelRemoves = Seq.empty))
      else
        None
    }

    // If any property is set in the ObjectConstructPattern or SetClause, we need to project the
    // variable in the resulting table. We also want to project this vertex.
    val allProjectAttrs: Set[Reference] = {
      val inlineRefs: Seq[Reference] = refsInExpression(newPropAssignments)
      val setClauseRefs: Seq[Reference] = {
        if (newSetClause.isDefined)
          refsInExpression(newSetClause.get)
        else
          Seq.empty
      }

      (inlineRefs ++ setClauseRefs :+ vertex.ref).toSet
    }

    val btableGrouping: RelationLike =
      processBindingTable(
        reference = vertex.ref,
        bindingTable = bindingTable,
        when = when,
        groupingAttributes = {
          if (isMatchRef)
            Seq(vertex.ref)
          else
            vertex.groupDeclaration.toList
        },
        aggregateFunctions = propAggregates,
        projectAttributes = allProjectAttrs)

    val vertexConstructRelation: RelationLike =
      VertexConstructRelation(
        reference = vertex.ref,
        relation = btableGrouping,
        expr = vertex.expr.copy(propAssignments = newPropAssignments),
        setClause = newSetClause,
        removeClause = removeClauseWithPropAggregates)

    vertexConstructRelation
  }

  /** Extracts the [[Reference]]s from all the [[PropertyRef]]s in a sub-tree. */
  private def refsInExpression(expr: AlgebraTreeNode): Seq[Reference] = {
    expr
      .preOrderMap {
        case propRef: PropertyRef => Some(propRef.ref)
        case _ => None
      }
      .filter(_.nonEmpty)
      .map(_.get)
  }

  /**
    * Replaces aggregates used in inline property assignments with new properties for a given
    * variable.
    */
  private def normalizePropAssignments(ref: Reference,
                                       propAssignments: PropAssignments)
  : (PropAssignments, Seq[PropertySet]) = {

    val exprToPropKey: mutable.ArrayBuffer[(AlgebraExpression, PropertyKey)] =
      mutable.ArrayBuffer[(AlgebraExpression, PropertyKey)]()

    val newProps: Seq[PropAssignment] =
      propAssignments.props.map(
        propAssignment =>
          if (hasAggregation(propAssignment.expr)) {
            val propKey: PropertyKey = PropertyKey(createVar(PROP_AGG_BASENAME))
            exprToPropKey += Tuple2(propAssignment.expr, propKey)
            PropAssignment(propAssignment.propKey, PropertyRef(ref, propKey))
          } else
            propAssignment
      )

    val newPropAssignments = PropAssignments(newProps)
    val propAggregates = createAggregatePropSets(ref, exprToPropKey)

    (newPropAssignments, propAggregates)
  }

  /**
    * Replaces aggregates used in SET property assignments with new properties for a given variable.
    */
  private def normalizeSetClause(ref: Reference,
                                 setClause: SetClause): (SetClause, Seq[PropertySet]) = {

    val exprToPropKey: mutable.ArrayBuffer[(AlgebraExpression, PropertyKey)] =
      mutable.ArrayBuffer[(AlgebraExpression, PropertyKey)]()

    val newPropSets: Seq[PropertySet] =
      setClause.propSets.map(
        propSet =>
          if (hasAggregation(propSet.propAssignment.expr)) {
            val propKey: PropertyKey = PropertyKey(createVar(PROP_AGG_BASENAME))
            exprToPropKey += Tuple2(propSet.propAssignment.expr, propKey)
            propSet.copy(
              propAssignment =
                PropAssignment(propSet.propAssignment.propKey, PropertyRef(ref, propKey)))
          } else
            propSet
      )

    val newSetClause = SetClause(newPropSets)
    val propAggregates = createAggregatePropSets(ref, exprToPropKey)

    (newSetClause, propAggregates)
  }

  /** Asserts whether there is an [[AggregateExpression]] in an [[AlgebraExpression]] sub-tree. */
  private def hasAggregation(expr: AlgebraExpression): Boolean = {
    var result: Boolean = false
    expr.forEachDown {
      case _: AggregateExpression => result = true
      case _ =>
    }

    result
  }

  /** Creates a [[PropertySet]] for each aggregation computed as a new property. */
  private def createAggregatePropSets(ref: Reference,
                                      exprToPropKey: Seq[(AlgebraExpression, PropertyKey)])
  : Seq[PropertySet] = {

    exprToPropKey
      .map {
        case (aggExpr, propKey) => PropertySet(ref, PropAssignment(propKey, aggExpr))
      }
  }

  /**
    * Splits a [[SetClause]] into individual [[SetClause]]s for each of the variables used in the
    * clause.
    */
  private def mapSetClauseToBinding(setClause: SetClause): Map[Reference, SetClause] = {
    setClause.propSets
      .map(propSet => (propSet.ref, propSet))
      // group by reference
      .groupBy(_._1)
      // extract the sequence of prop sets
      .mapValues(refPropSetTuples => refPropSetTuples.map(tuple => tuple._2))
      // create SetClauses from the sequence of PropSets
      .mapValues(SetClause)
  }

  /**
    * Splits a [[RemoveClause]] into individual [[RemoveClause]]s for each of the variables used in
    * the clause.
    */
  private def mapRemoveClauseToBinding(removeClause: RemoveClause): Map[Reference, RemoveClause] = {
    val bindingToPropRemove: Map[Reference, Seq[PropertyRemove]] =
      removeClause.propRemoves
        .map(propRemove => (propRemove.propertyRef.ref, propRemove))
        // group by reference
        .groupBy(_._1)
        // extract the sequence of prop removes
        .mapValues(refPropRemoveTuples => refPropRemoveTuples.map(tuple => tuple._2))

    val bindingToLabelRemove: Map[Reference, LabelRemove] =
      removeClause.labelRemoves
        .map(labelRemove => (labelRemove.ref, labelRemove.labelAssignments.labels))
        // group by reference
        .groupBy(_._1)
        // extract the sequences of labels
        .mapValues(refLabelsTuples => refLabelsTuples.flatMap(tuple => tuple._2))
        // create a LabelRemove from the reference and sequence of Labels
        .map { case (ref, labels) => ref -> LabelRemove(ref, LabelAssignments(labels)) }

    (bindingToPropRemove.keySet ++ bindingToLabelRemove.keySet) // merge keysets
      .map(
      ref => {
        val propRemoves: Seq[PropertyRemove] = bindingToPropRemove.getOrElse(ref, Seq.empty)
        val labelRemoves: Seq[LabelRemove] = {
          val optionLabelRemove: Option[LabelRemove] = bindingToLabelRemove.get(ref)
          if (optionLabelRemove.isDefined)
            Seq(optionLabelRemove.get)
          else
            Seq.empty
        }
        ref -> RemoveClause(propRemoves, labelRemoves)
      })
      .toMap
  }
}
