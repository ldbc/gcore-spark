package algebra.trees

import algebra.expressions._
import algebra.operators.BinaryOperator.reduceLeft
import algebra.operators._
import algebra.trees.BasicToGroupConstruct._
import algebra.types._
import common.RandomNameGenerator._
import common.trees.BottomUpRewriter
import parser.utils.VarBinder.createVar

import scala.collection.mutable

object BasicToGroupConstruct {

  /** Basename of aggregates that participate in a SET clause or inline property set. */
  val PROP_AGG_BASENAME: String = "propset_agg"

  val BTABLE_VIEW: String = s"BindingTable"
  val BASE_CONSTRUCT_VIEW_PREFIX: String = "BaseConstructView"
  val VERTEX_CONSTRUCT_VIEW_PREFIX: String = "VertexConstructView"
}

/**
  * Rewrites the CONSTRUCT sub-tree, such that each individual [[BasicConstructClause]] becomes its
  * own [[GroupConstruct]].
  *
  * The construction process begins with the [[BindingTable]], which is first filtered according to
  * the WHEN sub-clause of the [[BasicConstructClause]]. The [[BaseConstructTable]] view is then
  * used as a placeholder for the result of this filtering, as the restricted table will be used
  * multiple times in the construction process.
  *
  * An entity can be built from a variable that has been bound in the MATCH sub-clause so is now
  * present in the binding table, or from an unbound variable, that has no equivalent in the
  * binding table. Variables present in the binding table that are used in the CONSTRUCT clause need
  * to keep their original identity.
  *
  * --- Building a vertex ---
  * For a vertex built from a matched variable, the filtered binding table is grouped by that
  * vertex's identity ([[Reference]]). No other grouping is allowed in this case, as it would
  * violate the variable's identity, by replacing properties with their aggregates.
  *
  * An unbound vertex can use a specific [[GroupDeclaration]] to coalesce certain properties of the
  * binding table. However, if no GROUP-ing is specified in this case, no grouping is applied over
  * the binding table.
  *
  * An unbound vertex's column is not present in the binding table, thus, after the grouping (if
  * this is the case), a new column is added to the processed table, with the vertex's
  * [[Reference]].
  *
  * The above steps can be summarized into three cases:
  * (1) Building an entity that has been matched in the binding table:
  *   -> binding table is filtered;
  *   -> the result is grouped by vertex column (its identity).
  * (2) Building an unmatched vertex that has no additional GROUP-ing:
  *   -> binding table is filtered;
  *   -> a new column is added to the binding table, with that vertex's reference.
  * (3) Building an unmatched vertex that has an additional GROUP-ing clause:
  *   -> binding table is filtered;
  *   -> the result is grouped by the GROUP-ing attributes;
  *   -> a new column is added to the grouped table, with that vertex's reference.
  *
  * New properties and labels can be added or removed from the vertex, after the processing of the
  * binding table. The [[SetClause]] and the [[RemoveClause]] of the vertex will contain only those
  * properties or labels that have been specified for that particular vertex. For example, if we
  * build (c) and (f) and SET prop_c for vertex (c) and prop_f for vertex (f), then only prop_c will
  * be passed to c's [[ConstructRelation]] and only prop_f will be passed to f's
  * [[ConstructRelation]].
  *
  * New properties can be the result of an [[AggregateExpression]]. In this case, the
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
  * As part of a [[GroupConstruct]], a vertex denoted by its reference can appear multiple times in
  * the group, but with different properties or labels assigned to it. Therefore, we introduce a
  * normalization process, in which [[VertexConstruct]]s with the same reference, but different
  * features ([[GroupDeclaration]], copy pattern, [[ObjectConstructPattern]]) are merged into a
  * single entity with the specific reference. In this way, we avoid constructing the same entity
  * multiple times.
  *
  * -- Building a group of entities --
  * For [[BasicConstructClause]]s we use the following steps:
  * (1) We identify the unbound, ungrouped vertices in the construction topology. Iteratively, we
  * construct the vertices as in the previous paragraph and add them as new columns to the filtered
  * binding table.
  * (2) We identify the grouped vertices - bound vertices grouped by their identity or unbound
  * vertices, with GROUP-ing attributes. Iteratively, starting from the relation obtained in step
  * (1), we construct the vertices as described in the previous paragraph and apply an inner-join
  * with the relation at the preceding step of the iteration.
  *
  * The result of step (2) becomes the [[VertexConstructTable]] view that will be used to build the
  * edges.
  *
  * It is important to use correctly trimmed endpoint tables when joining in step (2), otherwise we
  * may end up with no attribute to join on or with too many attributes used for joining. A
  * [[ConstructRelation]] contains all the columns of the original binding table, plus new columns
  * for the new variables. If an endpoint was an unmatched variable with GROUP-ing attributes, then
  * its new table will contain less rows than the original binding table and some of the properties
  * of the matched entities may lose their meaning, due to aggregation. Moreover, new properties or
  * labels may have been added to matched entities, so a direct comparison between columns with the
  * same name in the original binding table and the table of the [[ConstructRelation]] may yield
  * false results.
  *
  * For example, let's suppose that after the MATCH (a)-[e]->(b) clause, we obtained the following
  * binding table:
  *
  *           +---+---+---+
  *           | a | e | b |
  * btable =  +---+---+---+
  *           |...|...|...|
  *
  * (2.1) We are solving the CONSTRUCT (a {newProp := expr})-[..]-(..) clause. This means we are
  * building a new vertex (a') from the matched vertex (a) as the endpoint of an edge, where:
  *   (a') := (a) + {newProp}
  *
  * When building the [[ConstructRelation]] of (a'), we will start from btable and perform all
  * the steps described for building a vertex. Then the resulting table will be stripped of all
  * other variables and we will only keep the new vertex (a'):
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
  * (2.2) We are solving the CONSTRUCT (x)-[..]-(..) clause. This means we are building an unbound
  * vertex (x) as the endpoint of an edge. In this case, we add x's new column to the original
  * binding table:
  *
  *            +---+---+---+---+
  *            | a | e | b | x |
  * btable_x = +---+---+---+---+
  *            |...|...|...|...|
  *
  * This will be the [[ConstructRelation]] of (x). No joining back with the original binding
  * table was needed in this case, which was treated in step (1).
  *
  * (2.3) We are solving the CONSTRUCT (x GROUP a.prop)-[..]-(..). This means we are building an
  * unbound vertex (x) with GROUP-ing on a.prop as the endpoint of an edge.
  *
  * The [[ConstructRelation]] of (x) will group the original binding table by the GROUP-ing
  * attributes. In this case, it will also have the [[ConstructRelation.groupedAttributes]]
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
  *
  * Once the [[VertexConstructTable]] has been built, edges can be added to it. We need to add the
  * vertices as a first step, because the edges are constructed based on grouped endpoint identity.
  *
  * -- Building an edge --
  * An edge, regardless of whether it has been previously matched, will be constructed by first
  * grouping the [[VertexConstructTable]] by the edge's endpoints. Properties and labels are
  * mutated exactly as for a vertex. From the resulting edge table, we project only the edge and its
  * endpoints. This projection is then joined with the starting table. If the edge had been matched,
  * then the joining will be performed on the edge identity, as well as on its endpoints' identity.
  * Otherwise, the joinining will be performed only on the endpoints' identity, as these are the
  * common variables between the starting table and the edge's table.
  *
  * In a [[GroupConstruct]] with multiple edges, the construction happens iteratively, starting from
  * the [[VertexConstructTable]].
  */
case class BasicToGroupConstruct(context: AlgebraContext)
  extends BottomUpRewriter[AlgebraTreeNode] {

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
      val constructRelations: Seq[GroupConstruct] =
        basicConstructs.map(createGroupConstruct(_, bindingToSetClause, bindingToRemoveClause))

      condConstruct.children = constructRelations
      construct.children = List(construct.graphs, condConstruct)
      construct
  }

  override val rule: RewriteFuncType = constructClause

  private
  def createGroupConstruct(basicConstruct: AlgebraTreeNode,
                           bindingToSetClause: Map[Reference, SetClause],
                           bindingToRemoveClause: Map[Reference, RemoveClause]): GroupConstruct = {
    val constructPattern: ConstructPattern =
      basicConstruct.children.head.asInstanceOf[ConstructPattern]
    val when: AlgebraExpression = basicConstruct.children.last.asInstanceOf[AlgebraExpression]

    val baseConstructViewName: String = s"${BASE_CONSTRUCT_VIEW_PREFIX}_${randomString()}"
    val vertexConstructViewName: String = s"${VERTEX_CONSTRUCT_VIEW_PREFIX}_${randomString()}"

    val filteredBindingTable: RelationLike = Select(relation = bindingTable, expr = when)
    val baseConstructTable: RelationLike =
      BaseConstructTable(baseConstructViewName, filteredBindingTable.getBindingSet)

    val vertexConstructs: Seq[SingleEndpointConstruct] =
      normalizeVertexConstructs(
        constructPattern.children
          .collect {
            case vertex: VertexConstruct => Seq(vertex)
            case edge: EdgeConstruct => Seq(edge.leftEndpoint, edge.rightEndpoint)
          }
          .flatten)
    val edgeConstructs: Seq[DoubleEndpointConstruct] =
      normalizeEdgeConstructs(
        constructPattern.children.collect {
          case edge: EdgeConstruct => edge
        })

    val vertexConstructTable: RelationLike =
      addVerticesToTable(baseConstructTable, vertexConstructs, bindingToSetClause)

    GroupConstruct(
      baseConstructTable = filteredBindingTable,
      vertexConstructTable = vertexConstructTable,
      baseConstructViewName = baseConstructViewName,
      vertexConstructViewName = vertexConstructViewName,
      edgeConstructTable = {
        if (edgeConstructs.isEmpty)
          RelationLike.empty
        else
          addEdgesToTable(
            VertexConstructTable(vertexConstructViewName, vertexConstructTable.getBindingSet),
            edgeConstructs, bindingToSetClause)
      },
      createRules = {
        val vertices: Seq[EntityCreateRule] =
          vertexConstructs.map(construct =>
            VertexCreate(construct.getRef, bindingToRemoveClause.get(construct.getRef)))
        val edges: Seq[EntityCreateRule] =
          edgeConstructs.map(construct =>
            EdgeCreate(
              construct.getRef,
              construct.getLeftEndpoint.getRef,
              construct.getRightEndpoint.getRef,
              construct.getConnType,
              bindingToRemoveClause.get(construct.getRef)))

        vertices ++ edges
      }
    )
  }

  /**
    * Merge [[VertexConstruct]]s with the same reference into the same [[VertexConstruct]] with
    * coalesced features.
    */
  private def normalizeVertexConstructs(connectionConstructs: Seq[SingleEndpointConstruct])
  : Seq[SingleEndpointConstruct] = {
    val constructMap: Map[Reference, Seq[SingleEndpointConstruct]] =
      connectionConstructs.groupBy(_.getRef)
    val normalizedConstructs: Seq[SingleEndpointConstruct] =
      constructMap
        .map {
          case (_, constructs) if constructs.size == 1 => constructs.head
          // TODO: Does it make sense to allow here multiple GROUP-ing?
          case (_, constructs) => constructs.reduce(_ merge _)
        }
        .toSeq
    normalizedConstructs
  }

  /**
    * Merge [[EdgeConstruct]]s with the same reference into the same [[EdgeConstruct]] with
    * coalesced features.
    */
  private def normalizeEdgeConstructs(connectionConstructs: Seq[DoubleEndpointConstruct])
  : Seq[DoubleEndpointConstruct] = {
    val constructMap: Map[Reference, Seq[DoubleEndpointConstruct]] =
      connectionConstructs.groupBy(_.getRef)
    val normalizedConstructs: Seq[DoubleEndpointConstruct] =
      constructMap
        .map {
          case (_, constructs) if constructs.size == 1 => constructs.head
          // TODO: Does it make sense to allow edge GROUP-ing?
          case (_, constructs) => constructs.reduce(_ merge _)
        }
        .toSeq
    normalizedConstructs
  }

  private def addVerticesToTable(baseBindingTable: RelationLike,
                                 connectionConstructs: Seq[SingleEndpointConstruct],
                                 bindingToSetClause: Map[Reference, SetClause]): RelationLike = {
    val constructsGroupingMap: Map[Reference, SingleEndpointConstruct] =
      connectionConstructs
        .filter(
          // grouping on identity or has GROUP
          construct => isMatchRef(construct.getRef) || construct.getGroupDeclaration.isDefined)
        .map(construct => construct.getRef -> construct)
        .toMap
    val constructsGrouping: Set[SingleEndpointConstruct] = constructsGroupingMap.values.toSet
    val constructsGroupingRefSet: Set[Reference] = constructsGroupingMap.keySet
    val constructsNoGrouping: Set[SingleEndpointConstruct] =
      connectionConstructs
        .filter(construct => !constructsGroupingRefSet.contains(construct.getRef))
        .toSet

    // Simply add new columns to the binding table, if the vertices are not grouping it. The
    // accumulator baseBindingTable is returned, if there are no ungrouped variables.
    val btableWithConstructsNoGrouping: RelationLike =
      constructsNoGrouping.foldLeft(baseBindingTable) {
        (relation, construct) =>
          newVertexConstruct(
            construct, relation, // add new construct to previous relation
            bindingToSetClause.get(construct.getRef))
      }

    // If vertices need grouping, then group the btable, then join back. If there had been no
    // ungrouped variables, then the btableWithConstructNoGrouping = baseBindingTable.
    val btableWithAllConstructs: RelationLike =
      reduceLeft(
        Seq(btableWithConstructsNoGrouping) ++ // start from table with vertices without grouping
          constructsGrouping.map(construct =>
            newVertexConstruct(
              construct, baseBindingTable, // we group the original btable and join back
              bindingToSetClause.get(construct.getRef))
          ),
        InnerJoin)

    btableWithAllConstructs
  }

  private def addEdgesToTable(vertexConstructTable: RelationLike,
                              connectionConstructs: Seq[DoubleEndpointConstruct],
                              bindingToSetClause: Map[Reference, SetClause]): RelationLike = {
    reduceLeft(
      Seq(vertexConstructTable) ++
        connectionConstructs.map(construct =>
          newEdgeConstruct(
            construct, vertexConstructTable,
            bindingToSetClause.get(construct.getRef))
        ),
      InnerJoin)
  }

  private def newVertexConstruct(vertexConstruct: SingleEndpointConstruct,
                                 bindingTable: RelationLike,
                                 setClause: Option[SetClause]): RelationLike = {
    val normalizePropertiesRes: (Seq[PropertySet], PropAssignments, Option[SetClause]) =
      normalizeProperties(vertexConstruct, setClause)
    val propAggregates: Seq[PropertySet] = normalizePropertiesRes._1
    val newPropAssignments: PropAssignments = normalizePropertiesRes._2
    val newSetClause: Option[SetClause] = normalizePropertiesRes._3

    // If we added any new properties to replace the aggregates, we need to remove them from the
    // final result.
    val aggPropRemoveClause: Option[RemoveClause] = propAggToRemoveClause(propAggregates)

    if (isMatchRef(vertexConstruct.getRef))
      // We only need to keep the constructed entity, if it has been matched, we can discard the
      // rest of the binding table for now.
      Project(
        relation =
          matchedVertexConstruction(
            vertexConstruct, bindingTable,
            propAggregates, newPropAssignments,
            newSetClause, aggPropRemoveClause),
        attributes = Set(vertexConstruct.getRef))
    else
      unmatchedVertexConstruction(
        vertexConstruct, bindingTable,
        propAggregates, newPropAssignments,
        newSetClause, aggPropRemoveClause)
  }

  private def matchedVertexConstruction(vertexConstruct: SingleEndpointConstruct,
                                        bindingTable: RelationLike,
                                        propAggregates: Seq[PropertySet],
                                        propAssignments: PropAssignments,
                                        setClause: Option[SetClause],
                                        removeClause: Option[RemoveClause]): ConstructRelation = {
    val btableGrouping: RelationLike =
      GroupBy(
        vertexConstruct.getRef,
        relation = bindingTable,
        groupingAttributes = Seq(vertexConstruct.getRef),
        aggregateFunctions = propAggregates)
    ConstructRelation(
      reference = vertexConstruct.getRef,
      isMatchedRef = true,
      relation = btableGrouping,
      expr = vertexConstruct.getExpr.copy(propAssignments = propAssignments),
      setClause = setClause,
      propAggRemoveClause = removeClause)
  }

  private def unmatchedVertexConstruction(vertexConstruct: SingleEndpointConstruct,
                                          bindingTable: RelationLike,
                                          propAggregates: Seq[PropertySet],
                                          propAssignments: PropAssignments,
                                          setClause: Option[SetClause],
                                          removeClause: Option[RemoveClause]): ConstructRelation = {
    val hasGrouping: Boolean = vertexConstruct.getGroupDeclaration.isDefined
    ConstructRelation(
      reference = vertexConstruct.getRef,
      isMatchedRef = false,
      relation =
        AddColumn(
          reference = vertexConstruct.getRef,
          relation = {
            if (hasGrouping)
              GroupBy(
                reference = vertexConstruct.getRef,
                relation = bindingTable,
                groupingAttributes = vertexConstruct.getGroupDeclaration.toList,
                aggregateFunctions = propAggregates)
            else
              bindingTable
          }),
      groupedAttributes = {
        if (hasGrouping) {
          val groupDeclaration: GroupDeclaration = vertexConstruct.getGroupDeclaration.get
          val propertyRefs: Seq[PropertyRef] =
            groupDeclaration.children.map(_.asInstanceOf[PropertyRef])
          propertyRefs
        }
        else
          Seq.empty
      },
      expr = vertexConstruct.getExpr.copy(propAssignments = propAssignments),
      setClause = setClause,
      propAggRemoveClause = removeClause)
  }

  private def newEdgeConstruct(edgeConstruct: DoubleEndpointConstruct,
                               bindingTable: RelationLike,
                               setClause: Option[SetClause]): RelationLike = {
    val normalizePropertiesRes: (Seq[PropertySet], PropAssignments, Option[SetClause]) =
      normalizeProperties(edgeConstruct, setClause)
    val propAggregates: Seq[PropertySet] = normalizePropertiesRes._1
    val newPropAssignments: PropAssignments = normalizePropertiesRes._2
    val newSetClause: Option[SetClause] = normalizePropertiesRes._3

    // If we added any new properties to replace the aggregates, we need to remove them from the
    // final result.
    val propAggRemoveClause: Option[RemoveClause] = propAggToRemoveClause(propAggregates)

    val btableGrouping: RelationLike =
      GroupBy(
        reference = edgeConstruct.getRef,
        relation = bindingTable,
        groupingAttributes =
          Seq(edgeConstruct.getLeftEndpoint.getRef, edgeConstruct.getRightEndpoint.getRef),
        aggregateFunctions = propAggregates)

    Project(
      relation =
        ConstructRelation(
          reference = edgeConstruct.getRef,
          isMatchedRef = isMatchRef(edgeConstruct.getRef),
          relation = {
            if (isMatchRef(edgeConstruct.getRef))
              btableGrouping
            else
              AddColumn(
                reference = edgeConstruct.getRef,
                relation = btableGrouping)
          },
          expr = edgeConstruct.getExpr.copy(propAssignments = newPropAssignments),
          setClause = newSetClause,
          propAggRemoveClause = propAggRemoveClause),
      attributes =
        Set(
          edgeConstruct.getRef,
          edgeConstruct.getLeftEndpoint.getRef,
          edgeConstruct.getRightEndpoint.getRef)
    )
  }

  private def isMatchRef(reference: Reference): Boolean =
    bindingContext.allRefs.contains(reference)

  /** Creates a [[RemoveClause]] to discard intermediate properties used for aggregation. */
  private def propAggToRemoveClause(propAggregates: Seq[PropertySet]): Option[RemoveClause] = {
    val removePropAggregates: Seq[PropertyRemove] =
      propAggregates.map(propertySet =>
        PropertyRemove(PropertyRef(propertySet.ref, propertySet.propAssignment.propKey)))

    if (removePropAggregates.nonEmpty)
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
      normalizePropAssignments(connection.getRef, connection.getExpr.propAssignments)
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
