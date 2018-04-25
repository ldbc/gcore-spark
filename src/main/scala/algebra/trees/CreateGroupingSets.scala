package algebra.trees

import algebra.expressions._
import algebra.operators._
import algebra.trees.CreateGroupingSets._
import algebra.types._
import common.trees.BottomUpRewriter
import parser.utils.VarBinder.createVar

import scala.collection.mutable

object CreateGroupingSets {

  /** Basename of aggregates that participate in a SET clause or inline property set. */
  val PROP_AGG_BASENAME: String = "propset_agg"
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
  * to keep their original identity.
  *
  * --- Building a graph entity ---
  * For an entity built from a matched variable, the filtered binding table is grouped by that
  * entity's identity ([[Reference]]). No other grouping is allowed in this case, as it would
  * violate the variable's identity, by replacing properties with their aggregates. In the case of
  * an edge, grouping by its reference is the same as grouping by its endpoints - the two endpoints
  * are strictly determined by the edge's identity.
  *
  * An unbound entity can use a specific [[GroupDeclaration]] to coalesce certain properties of the
  * binding table. However, if no GROUP-ing is specified in this case, no grouping is applied over
  * the binding table.
  *
  * An unbound entity's column is not present in the binding table, thus, after the grouping (if
  * this is the case), a new column is added to the processed table, with the entity's
  * [[Reference]].
  *
  * The above steps can be summarized into three cases:
  * (1) Building an entity that has been matched in the binding table:
  *   -> binding table is filtered;
  *   -> the result is grouped by entity column.
  * (2) Building an unmatched entity that has no additional GROUP-ing:
  *   -> binding table is filtered;
  *   -> a new column is added to the binding table, with that entity's reference.
  * (3) Building an unmatched entity that has an additional GROUP-ing clause:
  *   -> binding table is filtered;
  *   -> the result is grouped is grouped by the GROUP-ing attributes;
  *   -> a new column is added to the grouped table, with that entity's reference.
  *
  * New properties and labels can be added or removed from the entity, after the processing of the
  * binding table. The [[SetClause]] and the [[RemoveClause]] of the entity will contain only those
  * properties or labels that have been specified for that particular entity. For example, if we
  * build (c) and (f) and SET prop_c for vertex (c) and prop_f for vertex (f), then only prop_c will
  * be passed to c's [[EntityConstructRelation]] and only prop_f will be passed to f's
  * [[EntityConstructRelation]].
  *
  * New properties can be the result of an [[AggregateExpression]]. In this case, an
  * [[AggregateExpression]] (or, more specifically, the entire [[AlgebraExpression]] sub-tree that
  * contains that aggregation) is substituted with a new property assigned to the entity and
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
  *
  * --- Building a vertex ---
  * The vertex construct is represented by a [[VertexConstructRelation]]. The construction starts
  * from the binding table, which is first processed into an [[EntityConstructRelation]], as
  * previously described. From this new table, we project the vertex's [[Reference]]. The result is
  * the vertex's data.
  *
  * -- Building an edge --
  * The edge construct is represented by an [[EdgeConstructRelation]]. We first construct the two
  * endpoints of the edge by creating an [[EntityConstructRelation]] for each of them. Next, we
  * join the original binding table with the [[EntityConstructRelation]] of the left endpoint, then
  * we join to this result the [[EntityConstructRelation]] of the right endpoint. This results in a
  * binding table containing the new vertices and their respective properties and labels. The edge
  * is then constructed over this new table, with its own [[EntityConstructRelation]], exactly as
  * explained above. From this table, we project the edge's [[Reference]], as well as the
  * [[Reference]]s of its endpoints. The result is the edge's data.
  *
  * It is important to use correctly trimmed endpoint tables when joining back with the binding
  * table, otherwise we may end up with no attribute to join on or with too many attributes used for
  * joining. An [[EntityConstructRelation]] contains all the columns of the original binding table,
  * plus new columns for the new variables. If an endpoint was an unmatched variable with GROUP-ing
  * attributes, then its new table will contain less rows than the original binding table and some
  * of the properties of the matched entities may lose their meaning, due to aggregation. Moreover,
  * new properties or labels may have been added to matched entities, so a direct comparison between
  * columns with the same name in the original binding table and the table of the
  * [[EntityConstructRelation]] may yield false results.
  *
  * For example, let's suppose that after the MATCH (a)-[e]->(b) clause, we obtained the following
  * binding table:
  *
  *           +---+---+---+
  *           | a | e | b |
  * btable =  +---+---+---+
  *           |...|...|...|
  *
  * (1) We are solving the CONSTRUCT (a {newProp := expr})-[..]-(..) clause. This means we are
  * building a new vertex (a') from the matched vertex (a) as the endpoint of an edge, where:
  *   (a') := (a) + {newProp}
  *
  * When building the [[EntityConstructRelation]] of (a'), we will start from btable and perform all
  * the steps described for building a graph entity. Then the resulting table will be stripped from
  * all other variables and we will only keep the new vertex (a'):
  *
  *             +-------------------------+
  *             | (a') := (a) + {newProp} |
  * btable_a' = +-------------------------+
  *             |         ...             |
  *
  * When joining btable_a' with the original binding table, we should join on (a) = (a'). However,
  * (a) and (a') cannot be directly compared, because (a'), even though descending from (a),
  * contains a new property. The equality comparison, in this case should be done on common
  * properties of the two vertices.
  *
  * After the joining, the binding table will be:
  *
  *                    +---+---+----------------+
  *                    | e | b | a' := a + {..} |
  *  btable_join_a' =  +---+---+----------------+
  *                    |...|...|       ...      |
  *
  * and (a') will be "attached" to the pre-existing [e] and (b) columns, under the same relation/
  * order as (a) was.
  *
  * (2) We are solving the CONSTRUCT (x)-[..]-(..) clause. This means we are building an unbound
  * vertex (x) as the endpoint of an edge. In this case, we add x's new column to the original
  * binding table:
  *
  *            +---+---+---+---+
  *            | a | e | b | x |
  * btable_x = +---+---+---+---+
  *            |...|...|...|...|
  *
  * This will be the [[EntityConstructRelation]] of (x). No joining back with the original binding
  * table is needed in this case.
  *
  * (3) We are solving the CONSTRUCT (x GROUP a.prop)-[..]-(..). This means we are building an
  * unbound vertex (x) with GROUP-ing on a.prop as the endpoint of an edge.
  *
  * The [[EntityConstructRelation]] of (x) will group the original binding table by the GROUP-ing
  * attributes. In this case, it will also have the [[EntityConstructRelation.groupedAttributes]]
  * parameter set to the sequence of properties that have been used for grouping - a.prop in this
  * example.
  *
  *             +--------+--------+--------+--------------------+
  *             | AGG(a) | AGG(e) | AGG(b) | x := GROUP(a.prop) |
  *  btable_x = +--------+--------+--------+--------------------+
  *             |   ...  |   ...  |   ...  |         ...        |
  *
  *
  * The resulting table will contain the new column (x), but all other variables are aggregates, so
  * blindly joining on them will yield incorrect results. At target level, for this example, all
  * variables different from (x) and (a) should be discarded, as well as all properties of (a)
  * except for a.prop. Then the joining should be done on the common column (a), but again only
  * using a.prop for the equality comparison.
  *
  * After the joining, the binding table will be:
  *
  *                    +---+---+---+--------------------+
  *                    | a | e | b | x := GROUP(a.prop) |
  *  btable_join_a' =  +---+---+---+--------------------+
  *                    |...|...|...|         ...        |
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
        basicConstructs.flatMap(createGroupConstruct(_, bindingToSetClause, bindingToRemoveClause))

      condConstruct.children = constructRelations
      construct.children = List(construct.graphs, condConstruct)
      construct
  }

  override val rule: RewriteFuncType = constructClause

  private
  def createGroupConstruct(basicConstruct: AlgebraTreeNode,
                           bindingToSetClause: Map[Reference, SetClause],
                           bindingToRemoveClause: Map[Reference, RemoveClause])
  : Seq[RelationLike] = {

    val constructPattern: ConstructPattern =
      basicConstruct.children.head.asInstanceOf[ConstructPattern]
    val when: AlgebraExpression = basicConstruct.children.last.asInstanceOf[AlgebraExpression]
    val restrictedBindingTable: RelationLike = Select(relation = bindingTable, expr = when)

    val entityConstructs: Seq[ConstructRelation] =
      constructPattern.children.map {
        case vertex: VertexConstruct =>
          VertexConstructRelation(
            reference = vertex.ref,
            relation = {
              val vertexConstruct: RelationLike =
                newConstruct(
                  vertex, restrictedBindingTable,
                  bindingToSetClause.get(vertex.ref), bindingToRemoveClause.get(vertex.ref),
                  matchRefProjectAttrs = Set(vertex.ref))
              if (vertexConstruct.getBindingSet.refSet.size > 1) // more than the constructed vertex
                Project(
                  relation = vertexConstruct,
                  attributes = Set(vertex.ref))
              else
                vertexConstruct
            }
          )

        case edge: EdgeConstruct =>
          edgeRelation(
            edge, restrictedBindingTable,
            leftConstruct =
              newConstruct(
                edge.leftEndpoint, restrictedBindingTable,
                bindingToSetClause.get(edge.leftEndpoint.getRef),
                bindingToRemoveClause.get(edge.leftEndpoint.getRef),
                matchRefProjectAttrs = Set(edge.leftEndpoint.getRef)),
            rightConstruct =
              newConstruct(
                edge.rightEndpoint, restrictedBindingTable,
                bindingToSetClause.get(edge.rightEndpoint.getRef),
                bindingToRemoveClause.get(edge.rightEndpoint.getRef),
                matchRefProjectAttrs = Set(edge.rightEndpoint.getRef)),
            bindingToSetClause.get(edge.connName),
            bindingToRemoveClause.get(edge.connName))
      }

    entityConstructs
  }

  private def edgeRelation(edge: EdgeConstruct,
                           bindingTable: RelationLike,
                           leftConstruct: RelationLike,
                           rightConstruct: RelationLike,
                           setClause: Option[SetClause],
                           removeClause: Option[RemoveClause]): ConstructRelation = {

    val btableWithLeftAttr: RelationLike =
      InnerJoin(lhs = bindingTable, rhs = leftConstruct)
    val btableWithEndpoints: RelationLike =
      InnerJoin(lhs = btableWithLeftAttr, rhs = rightConstruct)

    val edgeConstruct: RelationLike =
      newConstruct(
        connectionConstruct = edge,
        bindingTable = btableWithEndpoints,
        setClause = setClause,
        removeClause = removeClause,
        matchRefProjectAttrs =
          Set(edge.getRef, edge.leftEndpoint.getRef, edge.rightEndpoint.getRef))

    EdgeConstructRelation(
      reference = edge.connName,
      relation =
        // TODO: Just like for the vertex, this can be done depending on the binding set of the
        // construct relation or if it was not matched ref (?).
        Project(
          relation = edgeConstruct,
          attributes = Set(edge.connName, edge.leftEndpoint.getRef, edge.rightEndpoint.getRef)),
      leftReference = edge.leftEndpoint.getRef,
      rightReference = edge.rightEndpoint.getRef,
      connType = edge.connType)
  }

  private def newConstruct(connectionConstruct: ConnectionConstruct,
                           bindingTable: RelationLike,
                           setClause: Option[SetClause],
                           removeClause: Option[RemoveClause],
                           matchRefProjectAttrs: Set[Reference]): RelationLike = {
    val normalizePropertiesRes: (Seq[PropertySet], PropAssignments, Option[SetClause]) =
      normalizeProperties(connectionConstruct, setClause)
    val propAggregates: Seq[PropertySet] = normalizePropertiesRes._1
    val newPropAssignments: PropAssignments = normalizePropertiesRes._2
    val newSetClause: Option[SetClause] = normalizePropertiesRes._3

    // If we added any new properties to replace the aggregates, we need to remove them from the
    // final result.
    val newRemoveClause: Option[RemoveClause] =
      addPropAggToRemoveClause(propAggregates, removeClause)

    if (isMatchRef(connectionConstruct.getRef))
      // We only need to keep the constructed entity, if it has been matched, we can discard the
      // rest of the binding table for now.
      Project(
        relation =
          matchedRefConstruction(
            connectionConstruct, bindingTable,
            propAggregates, newPropAssignments,
            newSetClause, newRemoveClause),
        attributes = matchRefProjectAttrs)
    else
      unmatchedRefConstruction(
        connectionConstruct, bindingTable,
        propAggregates, newPropAssignments,
        newSetClause, newRemoveClause)
  }

  private def matchedRefConstruction(connectionConstruct: ConnectionConstruct,
                                     bindingTable: RelationLike,
                                     propAggregates: Seq[PropertySet],
                                     propAssignments: PropAssignments,
                                     setClause: Option[SetClause],
                                     removeClause: Option[RemoveClause]): ConstructRelation = {
    val btableGrouping: RelationLike =
      GroupBy(
        connectionConstruct.getRef,
        relation = bindingTable,
        groupingAttributes = Seq(connectionConstruct.getRef),
        aggregateFunctions = propAggregates)
    EntityConstructRelation(
      reference = connectionConstruct.getRef,
      isMatchedRef = true,
      relation = btableGrouping,
      expr = connectionConstruct.getExpr.copy(propAssignments = propAssignments),
      setClause = setClause,
      removeClause = removeClause)
  }

  private def unmatchedRefConstruction(connectionConstruct: ConnectionConstruct,
                                       bindingTable: RelationLike,
                                       propAggregates: Seq[PropertySet],
                                       propAssignments: PropAssignments,
                                       setClause: Option[SetClause],
                                       removeClause: Option[RemoveClause]): ConstructRelation = {
    val hasGrouping: Boolean = connectionConstruct.getGroupDeclaration.isDefined
    EntityConstructRelation(
      reference = connectionConstruct.getRef,
      isMatchedRef = false,
      relation =
        AddColumn(
          reference = connectionConstruct.getRef,
          relation = {
            if (hasGrouping)
              GroupBy(
                connectionConstruct.getRef,
                relation = bindingTable,
                connectionConstruct.getGroupDeclaration.toList,
                propAggregates)
            else
              bindingTable
          }),
      groupedAttributes = {
        if (hasGrouping) {
          val groupDeclaration: GroupDeclaration = connectionConstruct.getGroupDeclaration.get
          val propertyRefs: Seq[PropertyRef] =
            groupDeclaration.children.map(_.asInstanceOf[PropertyRef])
          propertyRefs
        }
        else
          Seq.empty
      },
      expr = connectionConstruct.getExpr.copy(propAssignments = propAssignments),
      setClause = setClause,
      removeClause = removeClause)
  }

  private def isMatchRef(reference: Reference): Boolean =
    bindingContext.allRefs.contains(reference)

  private def addPropAggToRemoveClause(propAggregates: Seq[PropertySet],
                                       removeClause: Option[RemoveClause]): Option[RemoveClause] = {
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

  private def normalizeProperties(connection: ConnectionConstruct,
                                  setClause: Option[SetClause])
  : (Seq[PropertySet], PropAssignments, Option[SetClause]) = {

    // For inline property assignments, pull out aggregations and replace with new property for this
    // vertex.
    val normalizePropAssignmentsRes: (PropAssignments, Seq[PropertySet]) =
    normalizePropAssignments(
      connection.getRef, connection.getExpr.propAssignments)
    // For SET property assignments, pull out aggregations and replace with new property for this
    // vertex.
    val normalizeSetClauseRes: Option[(SetClause, Seq[PropertySet])] = {
      if (setClause.isDefined)
        Some(normalizeSetClause(connection.getRef, setClause.get))
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

    (propAggregates, newPropAssignments, newSetClause)
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
