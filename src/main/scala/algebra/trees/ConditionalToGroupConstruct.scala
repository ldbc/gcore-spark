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

import algebra.expressions._
import algebra.operators._
import algebra.trees.ConditionalToGroupConstruct._
import algebra.types._
import common.RandomNameGenerator._
import common.trees.BottomUpRewriter

object ConditionalToGroupConstruct {
  /**
    * The view name of the binding table (right after solving MATCH). Currently used by the target.
    */
  val BTABLE_VIEW: String = s"BindingTable"

  /** The prefix for the view name of the filtered binding table of a [[CondConstructClause]]. **/
  val BASE_CONSTRUCT_VIEW_PREFIX: String = "BaseConstructView"

  /**
    * The prefix for the view name of the table containing all the vertices of a
    * [[CondConstructClause]]. The view is the base construct table for the edges of the same
    * clause.
    */
  val VERTEX_CONSTRUCT_VIEW_PREFIX: String = "VertexConstructView"

  /**
    * Unmatched ungrouped vertices are built iteratively. This prefix is used to create a view name
    * for the table to which a new vertex will be added. For the first vertex, this will be the
    * filtered binding table, for the second vertex it will be the filtered binding table + the
    * first vertex, and so on.
    */
  val CONSTRUCT_REL_VIEW_PREFIX: String = "ConstructRelationView"
}

/**
  * TODO: Add unit tests for this class.
  *
  * Rewrites the CONSTRUCT sub-tree, such that each individual [[CondConstructClause]] becomes its
  * own [[GroupConstruct]].
  *
  * The construction process begins with the binding table, which is first filtered according to
  * the WHEN sub-clause of the [[CondConstructClause]]. The [[BaseConstructTableView]] view is then
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
  * binding table. A [[ConstructRule]] is used for each new vertex (matched or unmatched) to specify
  * what to add or remove to the entity. The [[ConstructRule.setAssignments]] and the
  * [[ConstructRule.removeAssignments]] of the vertex's [[ConstructRule]] will contain only those
  * properties or labels that have been specified for that particular vertex in the entire construct
  * clause. For example, if we build (c) and (f) and SET prop_c for vertex (c) and prop_f for vertex
  * (f), then only prop_c will be passed to c's [[ConstructRule]] and only prop_f will be passed to
  * f's [[ConstructRule]].
  *
  * New properties can be the result of an [[AggregateExpression]]. This is only allowed for
  * unmatched and GROUP-ed vertices, as otherwise we would violate the identity of matched vertices
  * or it wouldn't make sense to use aggregates for unmatched unGROUP-ed vertices (as there is no
  * grouping involved in their construction). New aggregate properties will be computed through the
  * [[GroupBy]] operation that creates the respective vertex.
  *
  * -- Building a group of entities --
  * For each [[CondConstructClause]] we use the steps below to build the new PPG. Note that these
  * steps are performed jointly between the algebra and the target module.
  * (1) We build the vertices first:
  *   (a) For each unmatched unGROUP-ed vertex, a new column is added to the base construct table
  *   (the filtered binding table). In the algebra, we simply create the algebraic plan for this and
  *   the target is responsible for actually solving the column addition.
  *   (b) To build the matched vertices, we group the filtered binding table by their identity. No
  *   other action is required in this case.
  *   (c) To build the unmatched GROUP-ed vertices, we start from the filtered binding table that
  *   contains the vertices added in (a), because it will already contain the vertices from (b). For
  *   each new vertex in this case, we group the binding table as specified by the construct
  *   pattern and add a column with the new vertex identities. The target must then ensure that this
  *   grouped table with fresh identities is joined back to the base binding table.
  * (2) Once we have all the vertices into one table, we proceed to building the edges:
  *   (a) Matched edges require grouping the vertex construct table by the edge's and its endpoints'
  *   identity.
  *   (b) Unmatched edges are built by grouping the vertex construct table by the endpoints'
  *   identity.
  *
  * It is important to use correctly trimmed endpoint tables when joining in step (1.c), otherwise
  * we may end up with no attribute to join on or with too many attributes used for joining. If an
  * endpoint was an unmatched variable with GROUP-ing attributes, then its new table will contain
  * less rows than the original binding table and some of the properties of the matched entities may
  * lose their meaning, due to aggregation. A direct comparison between columns with the same name
  * in the original binding table and the grouped table of the new vertex may yield false results.
  *
  * For example, let's suppose that after the MATCH (a)-[e]->(b) clause, we obtained the following
  * binding table:
  *
  *           +---+---+---+
  *           | a | e | b |
  * btable =  +---+---+---+
  *           |...|...|...|
  *
  * We are solving the CONSTRUCT (x GROUP a.prop)-[..]-(..). This means we are building an unbound
  * vertex (x) with GROUP-ing on a.prop as the endpoint of an edge.
  *
  * The [[ConstructRule.constructRelation]] of (x) will group the original binding table by the
  * GROUP-ing attributes. In this case, it will also have the [[ConstructRule.groupDeclaration]]
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
  */
case class ConditionalToGroupConstruct(context: AlgebraContext)
  extends BottomUpRewriter[AlgebraTreeNode] {

  assert(context.bindingContext.isDefined,
    "The bindingContext in AlgebraContext needs to be defined for this rewrite stage.")

  private val bindingContext: BindingContext = context.bindingContext.get
  private val bindingTableView: BindingTableView =
    BindingTableView(BindingSet(bindingContext.allRefs))

  private val constructClause: RewriteFuncType = {
    case construct: ConstructClause =>
      construct.children.foreach(constructPattern=>{
        val setClause: SetClause = constructPattern.children(3).asInstanceOf[SetClause]
        val removeClause: RemoveClause = constructPattern.children(4).asInstanceOf[RemoveClause]
        val refToSetAssignments: Map[Reference, Seq[AlgebraExpression]] = mapRefToPropSets(setClause)
        val refToRemoveAssignments: Map[Reference, Seq[AlgebraExpression]] =
          mapRefToRemoves(removeClause)
        //val where = construct.where

        // Rewrite construct's children to group constructs.
        val condConstructs: Seq[AlgebraTreeNode] = constructPattern.children(1).children
        val groupConstructs: Seq[GroupConstruct] =
          condConstructs.map(condConstruct =>
            createGroupConstruct(
              condConstruct.asInstanceOf[CondConstructClause],
              refToSetAssignments, refToRemoveAssignments))

        constructPattern.children = groupConstructs

      })

      construct
  }

  override val rule: RewriteFuncType = constructClause

  /** Creates a [[GroupConstruct]] from the given [[CondConstructClause]]. */
  private def createGroupConstruct(condConstructClause: CondConstructClause,
                                   refToSetAssignments: Map[Reference, Seq[AlgebraExpression]],
                                   refToRemoveAssignments: Map[Reference, Seq[AlgebraExpression]])
  : GroupConstruct = {

    val constructPattern: ConstructPattern =
      condConstructClause.children.head.asInstanceOf[ConstructPattern]
    //val when: AlgebraExpression = constructPattern. // ex when where aqui

    val baseConstructViewName: String = s"${BASE_CONSTRUCT_VIEW_PREFIX}_${randomString()}"
    val vertexConstructViewName: String = s"${VERTEX_CONSTRUCT_VIEW_PREFIX}_${randomString()}"

    // Filter the binding table by the expression in WHEN.
    val filteredBindingTable: RelationLike = Select(relation = bindingTableView, expr = True)
    val baseConstructTableView: BaseConstructTableView =
      BaseConstructTableView(baseConstructViewName, filteredBindingTable.getBindingSet)

    // Create a mapping between each variable in the construct pattern and its respective label and
    // property SET assignments.
    val patternRefToSetAssignments: Map[Reference, Seq[AlgebraExpression]] =
    mapRefToSets(constructPattern)

    // Separate vertices by unmatched grouped, unmatched ungrouped and matched.
    val vertexConstructs: Seq[SingleEndpointConstruct] =
      constructPattern.topology
        .collect {
          case vertex: VertexConstruct => Seq(vertex)
          case edge: EdgeConstruct => Seq(edge.leftEndpoint, edge.rightEndpoint)
        }
        .flatten
    val unmatchedUngroupedVertices: Seq[Reference] =
      vertexConstructs
        .filter(vertex => !isMatchRef(vertex.getRef) && vertex.getGroupDeclaration.isEmpty)
        .map(_.getRef)
        .distinct
    val unmatchedGroupedVertices: Map[Reference, GroupDeclaration] =
      vertexConstructs
        .filter(vertex => !isMatchRef(vertex.getRef) && vertex.getGroupDeclaration.isDefined)
        .map(vertex => vertex.getRef -> vertex)
        // group by vertex reference
        .groupBy(_._1)
        // extract sequence of VertexConstruct
        .mapValues(seqTuples => seqTuples.map(_._2))
        .mapValues(vertexConstructs => {
          // TODO: Here we extract the first GroupDeclaration we encounter. Should we throw an error
          // if there is more than one GD? What are the semantics of multiple GDs in a single
          // graph construct pattern?
          vertexConstructs.filter(_.getGroupDeclaration.isDefined).head.getGroupDeclaration.get
        })
    val matchedVertices: Seq[Reference] =
      vertexConstructs.filter(vertex => isMatchRef(vertex.getRef)).map(_.getRef).distinct

    // Separate edges by matched and unmatched.
    val edgeConstructs: Seq[(Reference, Reference, Reference)] =
      constructPattern.topology
        .collect { case edge: EdgeConstruct => edge }
        .map(edgeConstruct => {
          val (srcRef, dstRef): (Reference, Reference) = edgeConstruct.connType match {
            case OutConn => (edgeConstruct.leftEndpoint.getRef, edgeConstruct.rightEndpoint.getRef)
            case InConn => (edgeConstruct.rightEndpoint.getRef, edgeConstruct.leftEndpoint.getRef)
          }
          (edgeConstruct.connName, srcRef, dstRef)
        })
        .distinct
    val matchedEdges: Seq[(Reference, Reference, Reference)] =
      edgeConstructs.filter { case (edgeRef, _, _) => isMatchRef(edgeRef) }
    val unmatchedEdges: Seq[(Reference, Reference, Reference)] =
      edgeConstructs.filterNot { case (edgeRef, _, _) => isMatchRef(edgeRef) }

    // Create for each vertex the construct rule and add to VertexConstructTable. Each rule receives
    // the SET and REMOVE assignments from the construct clause + the assignments from the pattern.
    val unmatchedUngroupedRules: Seq[ConstructRule] =
    unmatchedUngroupedVertices.foldLeft(
      (Seq.empty[ConstructRule], baseConstructTableView.asInstanceOf[TableView])) {
      case ((accumulator, prevTableView), reference) =>
        val constructRelation: RelationLike = AddColumn(reference, prevTableView)
        val constructRelationTableView: ConstructRelationTableView =
          ConstructRelationTableView(
            s"${CONSTRUCT_REL_VIEW_PREFIX}_${randomString()}",
            constructRelation.getBindingSet)
        val constructRule: ConstructRule =
          ConstructRule(
            reference,
            refToSetAssignments.getOrElse(reference, Seq.empty) ++
              patternRefToSetAssignments.getOrElse(reference, Seq.empty),
            refToRemoveAssignments.getOrElse(reference, Seq.empty),
            constructRelation,
            Some(constructRelationTableView))

        (accumulator :+ constructRule, constructRelationTableView)
    }._1
    val vertexConstructTable: VertexConstructTable =
      VertexConstructTable(
        unmatchedUngroupedRules,
        unmatchedGroupedVertices
          .map {
            case (reference, groupDeclaration) =>
              val setAssignments: Seq[AlgebraExpression] =
                refToSetAssignments.getOrElse(reference, Seq.empty) ++
                  patternRefToSetAssignments.getOrElse(reference, Seq.empty)
              // Aggregates can only be PropertySets.
              val aggregates: Seq[PropertySet] =
                setAssignments.filter(hasAggregation).map(_.asInstanceOf[PropertySet])
              val nonAggregates: Seq[AlgebraExpression] = setAssignments.filterNot(hasAggregation)

              val constructRelation: RelationLike =
                AddColumn(
                  reference,
                  GroupBy(
                    reference = reference,
                    relation = baseConstructTableView,
                    groupingAttributes = groupDeclaration.groupingSets,
                    aggregateFunctions = aggregates))
              val constructRelationTableView: ConstructRelationTableView =
                ConstructRelationTableView(
                  s"${CONSTRUCT_REL_VIEW_PREFIX}_${randomString()}",
                  constructRelation.getBindingSet)
              ConstructRule(
                reference,
                nonAggregates,
                refToRemoveAssignments.getOrElse(reference, Seq.empty),
                constructRelation,
                Some(constructRelationTableView),
                Some(groupDeclaration))
          }.toSeq,
        matchedVertices
          .map(reference => {
            val constructRelation: RelationLike =
              GroupBy(
                reference = reference,
                relation = baseConstructTableView,
                groupingAttributes = List(reference),
                aggregateFunctions = Seq.empty)
            val constructRelationTableView: ConstructRelationTableView =
              ConstructRelationTableView(
                s"${CONSTRUCT_REL_VIEW_PREFIX}_${randomString()}",
                constructRelation.getBindingSet)
            ConstructRule(
              reference,
              refToSetAssignments.getOrElse(reference, Seq.empty) ++
                patternRefToSetAssignments.getOrElse(reference, Seq.empty),
              refToRemoveAssignments.getOrElse(reference, Seq.empty),
              constructRelation,
              Some(constructRelationTableView))
          })
      )
    val vertexConstructTableView: VertexConstructTableView =
      VertexConstructTableView(vertexConstructViewName, vertexConstructTable.getBindingSet)

    // Create for each edge the construct rule and add to VertexConstructTable. Each rule receives
    // the SET and REMOVE assignments from the construct clause + the assignments from the pattern.
    val edgeConstructRules: Seq[ConstructRule] =
    matchedEdges.map { case (edgeRef, srcRef, dstRef) =>
      ConstructRule(
        reference = edgeRef,
        setAssignments =
          refToSetAssignments.getOrElse(edgeRef, Seq.empty) ++
            patternRefToSetAssignments.getOrElse(edgeRef, Seq.empty),
        removeAssignments = refToRemoveAssignments.getOrElse(edgeRef, Seq.empty),
        constructRelation =
          GroupBy(
            reference = edgeRef,
            relation = vertexConstructTableView,
            groupingAttributes = Seq(edgeRef, srcRef, dstRef),
            aggregateFunctions = Seq.empty),
        fromRef = Some(srcRef),
        toRef = Some(dstRef))
    } ++
      unmatchedEdges.map { case (edgeRef, srcRef, dstRef) =>
        ConstructRule(
          reference = edgeRef,
          setAssignments =
            refToSetAssignments.getOrElse(edgeRef, Seq.empty) ++
              patternRefToSetAssignments.getOrElse(edgeRef, Seq.empty),
          removeAssignments = refToRemoveAssignments.getOrElse(edgeRef, Seq.empty),
          constructRelation =
            AddColumn(
              reference = edgeRef,
              relation =
                GroupBy(
                  reference = edgeRef,
                  relation = vertexConstructTableView,
                  groupingAttributes = Seq(srcRef, dstRef),
                  aggregateFunctions = Seq.empty)),
          fromRef = Some(srcRef),
          toRef = Some(dstRef))
      }

    // Create and return group construct.
    GroupConstruct(
      filteredBindingTable, baseConstructTableView,
      vertexConstructTable, vertexConstructTableView,
      edgeConstructRules)
  }

  /**
    * Creates a mapping between the variables in a [[ConstructPattern]] and their respective label
    * and property SET assignments in the same [[ConstructPattern]].
    */
  private def mapRefToSets(pattern: ConstructPattern): Map[Reference, Seq[AlgebraExpression]] = {
    pattern.topology
      .flatMap {
        case VertexConstruct(ref, _, _, expr) => mapRefToSets(ref, expr)

        case EdgeConstruct(
        ref, _,
        VertexConstruct(leftRef, _, _, leftExpr), VertexConstruct(rightRef, _, _, rightExpr),
        _, _, expr) =>
          mapRefToSets(ref, expr) ++ mapRefToSets(leftRef, leftExpr) ++
            mapRefToSets(rightRef, rightExpr)
      }
      // group by reference
      .groupBy(_._1) // Map[Reference, Seq[(Reference, Seq[AlgebraExpression])]]
      // extract the sequence of expressions for each reference
      .mapValues(seqTuples => seqTuples.map(_._2).distinct)
  }

  private def mapRefToSets(ref: Reference,
                           expr: ObjectConstructPattern): Seq[(Reference, AlgebraExpression)] = {
    val propAssignments: Seq[(Reference, AlgebraExpression)] =
      expr.propAssignments.props.map(prop => ref -> PropertySet(ref, prop))
    val labelAssignments: (Reference, AlgebraExpression) = (ref, expr.labelAssignments)
    propAssignments :+ labelAssignments
  }

  /**
    * Creates a mapping between the variables used in the property SET assignments under the SET
    * clause and their respective assignments.
    */
  private def mapRefToPropSets(setClause: SetClause): Map[Reference, Seq[AlgebraExpression]] = {
    setClause.propSets
      .map(propSet => (propSet.ref, propSet))
      // group by reference
      .groupBy(_._1)
      // extract the sequence of prop sets
      .mapValues(refPropSetTuples => refPropSetTuples.map(tuple => tuple._2))
  }

  /**
    * Creates a mapping between the variables used in the property and label REMOVE assignments
    * under the REMOVE clause and their respective assignments.
    */
  private
  def mapRefToRemoves(removeClause: RemoveClause): Map[Reference, Seq[AlgebraExpression]] = {
    val removes: Seq[AlgebraExpression] = removeClause.labelRemoves ++ removeClause.propRemoves
    removes
      .map {
        case propRemove @ PropertyRemove(PropertyRef(ref, _)) => ref -> propRemove
        case labelRemove @ LabelRemove(ref, _) => ref -> labelRemove
      }
      // group by reference
      .groupBy(_._1)
      // extract the sequence of prop/label removes from the value
      .mapValues(refRemoveTuples => refRemoveTuples.map(tuple => tuple._2))
  }

  /** Asserts whether a variable has been already used in the MATCH clause. */
  private def isMatchRef(reference: Reference): Boolean =
    bindingContext.allRefs.contains(reference)

  /** Asserts whether there is an [[AggregateExpression]] in an [[AlgebraExpression]] sub-tree. */
  private def hasAggregation(expr: AlgebraExpression): Boolean = {
    var result: Boolean = false
    expr.forEachDown {
      case _: AggregateExpression => result = true
      case _ =>
    }

    result
  }
}
