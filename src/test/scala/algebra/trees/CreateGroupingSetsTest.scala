package algebra.trees

import algebra.expressions._
import algebra.operators._
import algebra.trees.CustomMatchers._
import algebra.trees.CreateGroupingSets.PROP_AGG_BASENAME
import algebra.types._
import org.scalatest.{FunSuite, Inside, Matchers}
import parser.utils.VarBinder
import schema.GraphDb

import scala.collection.mutable

class CreateGroupingSetsTest extends FunSuite with Matchers with Inside {

  /**
    *                 +---+---+---+
    *                 | c | e | f |
    * bindingTable =  +---+---+---+
    *                 |...|...|...|
    */
  private val bindingTable: BindingTable =
    BindingTable(new BindingSet(Reference("c"), Reference("e"), Reference("f")))

  private val bindingContext: BindingContext =
    BindingContext(
      vertexBindings = Set(Reference("c"), Reference("f")),
      edgeBindings = Set(ReferenceTuple(Reference("e"), Reference("c"), Reference("f"))),
      pathBindings = Set.empty)

  private val rewriter: CreateGroupingSets =
    CreateGroupingSets(
      AlgebraContext(GraphDb.empty, bindingToGraph = None, bindingContext = Some(bindingContext)))

  private val emptySetClause: SetClause = SetClause(Seq.empty)
  private val emptyRemoveClause: RemoveClause =
    RemoveClause(labelRemoves = Seq.empty, propRemoves = Seq.empty)

  test("There are as many GroupConstruct as BasicConstructClause") {
    val vertex1 =
      VertexConstruct(
        Reference("v1"),
        copyPattern = None,
        groupDeclaration = None,
        expr = ObjectConstructPattern.empty)
    val vertex2 = vertex1.copy(ref = Reference("v2"))
    val constructClause =
      ConstructClause(
        graphs = GraphUnion(Seq.empty),
        CondConstructClause(
          condConstructs = Seq(
            BasicConstructClause(ConstructPattern(Seq(vertex1)), when = True),
            BasicConstructClause(ConstructPattern(Seq(vertex2)), when = True)
          )
        ),
        emptySetClause, emptyRemoveClause)

    extractConstructRelations(
      constructTree = rewriter rewriteTree constructClause,
      expectedNumConstructs = 2)
  }

  test("Create vertex from bound variable - CONSTRUCT (c)") {
    val algebraTree = vertexConstruct(reference = Reference("c"))
    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    actual should matchGroupConstructBoundVertex(reference = Reference("c"))
  }

  test("Create vertex from bound variable, inline prop and label (object construct pattern) - " +
    "CONSTRUCT (c :NewLabel {newProp := 1})") {
    val objectConstructPattern =
      ObjectConstructPattern(
        labelAssignments = LabelAssignments(Seq(Label("NewLabel"))),
        propAssignments =
          PropAssignments(Seq(
            PropAssignment(PropertyKey("newProp"), IntLiteral(1))))
      )
    val algebraTree =
      vertexConstruct(reference = Reference("c"), objConstructPattern = objectConstructPattern)

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    actual should matchGroupConstructBoundVertex(
      reference = Reference("c"),
      objectConstructPattern = objectConstructPattern)
  }

  test("Create vertex from bound variable, only properties SET for (c) are passed to relation - " +
    "CONSTRUCT (c) SET c.prop1 := 1 SET f.prop2 := 2 SET c.prop3 := 3") {
    val prop1 = PropertySet(Reference("c"), PropAssignment(PropertyKey("prop1"), IntLiteral(1)))
    val prop2 = PropertySet(Reference("c"), PropAssignment(PropertyKey("prop2"), IntLiteral(2)))
    val prop3 = PropertySet(Reference("f"), PropAssignment(PropertyKey("prop1"), IntLiteral(3)))
    val algebraTree =
      vertexConstruct(reference = Reference("c"), setClause = SetClause(Seq(prop1, prop2, prop3)))

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    actual should matchGroupConstructBoundVertex(
      reference = Reference("c"),
      setClause = Some(SetClause(Seq(prop1, prop2)))
    )
  }

  test("Create vertex from bound variable, only properties and labels REMOVEd from (c) are passed" +
    " to relation - CONSTRUCT (c) REMOVE c.prop1 REMOVE f.prop2 REMOVE c:Label1 REMOVE f:Label2") {
    val prop1 = PropertyRemove(PropertyRef(Reference("c"), PropertyKey("prop1")))
    val prop2 = PropertyRemove(PropertyRef(Reference("f"), PropertyKey("prop2")))
    val label1 = LabelRemove(Reference("c"), LabelAssignments(Seq(Label("Label1"))))
    val label2 = LabelRemove(Reference("f"), LabelAssignments(Seq(Label("Label2"))))
    val algebraTree =
      vertexConstruct(
        reference = Reference("c"),
        removeClause =
          RemoveClause(
            propRemoves = Seq(prop1, prop2),
            labelRemoves = Seq(label1, label2))
      )

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    actual should matchGroupConstructBoundVertex(
      reference = Reference("c"),
      removeClause = Some(RemoveClause(propRemoves = Seq(prop1), labelRemoves = Seq(label1)))
    )
  }

  test("Create vertex from bound variable, WHEN condition filters the binding table - " +
    "CONSTRUCT (c) WHEN f.prop1 > 2") {
    val condition = Gt(PropertyRef(Reference("f"), PropertyKey("prop1")), IntLiteral(2))
    val algebraTree = vertexConstruct(reference = Reference("c"), when = condition)
    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    actual should matchGroupConstructBoundVertex(
      reference = Reference("c"),
      when = condition)
  }

  test("Create vertex from unbound variable - CONSTRUCT (x)") {
    val algebraTree = vertexConstruct(reference = Reference("x"))
    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    actual should matchGroupConstructUnboundVertex(reference = Reference("x"))
  }

  test("Create vertex from unbound variable, GROUPing attributes are passed to GroupBy - " +
    "CONSTRUCT (x GROUP c.prop1)") {
    val groupingProps = Seq(PropertyRef(Reference("c"), PropertyKey("prop1")))
    val algebraTree =
      vertexConstruct(
        reference = Reference("x"),
        groupDeclaration = Some(GroupDeclaration(groupingProps)))

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    actual should matchGroupConstructUnboundGroupedVertex(
      reference = Reference("x"),
      groupingProps = groupingProps)
  }

  test("Create vertex from unbound variable, attributes used in inline prop and prop SET are " +
    "passed to Project - CONSTRUCT (x {prop1 := 2 * c.prop1}) SET x.prop2 := f.prop1)") {
    val objConstructPattern =
      ObjectConstructPattern(
        labelAssignments = LabelAssignments(Seq.empty),
        propAssignments =
          PropAssignments(Seq(
            PropAssignment(
              PropertyKey("prop1"),
              Mul(PropertyRef(Reference("c"), PropertyKey("prop1")), IntLiteral(2)))
          )))
    val setClause =
      SetClause(Seq(
        PropertySet(
          Reference("x"),
          PropAssignment(
            PropertyKey("prop2"),
            PropertyRef(Reference("f"), PropertyKey("prop1"))))
      ))
    val algebraTree =
      vertexConstruct(
        reference = Reference("x"),
        objConstructPattern = objConstructPattern,
        setClause = setClause)

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    actual should matchGroupConstructUnboundVertex(
        reference = Reference("x"),
        objectConstructPattern = objConstructPattern,
        setClause = Some(setClause))
  }

  test("Create vertex from unbound variable, aggregates are passed to GroupBy and replaced with " +
    "a PropertyRef inline or in SET - " +
    "CONSTRUCT (x GROUP c.prop3 {prop1 := AVG(c.prop1)}) SET x.prop2 := MIN(c.prop2)") {
    VarBinder.reset()
    val groupingProps = Seq(PropertyRef(Reference("c"), PropertyKey("prop3")))
    val avg = Avg(distinct = false, PropertyRef(Reference("c"), PropertyKey("prop1")))
    val min = Min(distinct = false, PropertyRef(Reference("c"), PropertyKey("prop2")))
    val algebraTree =
      vertexConstruct(
        reference = Reference("x"),
        groupDeclaration = Some(GroupDeclaration(groupingProps)),
        objConstructPattern =
          ObjectConstructPattern(
            labelAssignments = LabelAssignments(Seq.empty),
            propAssignments = PropAssignments(Seq(PropAssignment(PropertyKey("prop1"), avg)))
          ),
        setClause =
          SetClause(Seq(PropertySet(Reference("x"), PropAssignment(PropertyKey("prop2"), min)))))

    val avgPropAlias = PropertyKey(s"${PROP_AGG_BASENAME}_0")
    val minPropAlias = PropertyKey(s"${PROP_AGG_BASENAME}_1")
    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    actual should matchGroupConstructUnboundGroupedVertex(
      reference = Reference("x"),
      groupingProps = groupingProps,
      aggregateFunctions = Seq(
        PropertySet(Reference("x"), PropAssignment(avgPropAlias, avg)),
        PropertySet(Reference("x"), PropAssignment(minPropAlias, min))),
      objectConstructPattern =
        ObjectConstructPattern(
          labelAssignments = LabelAssignments(Seq.empty),
          propAssignments =
            PropAssignments(Seq(
              PropAssignment(PropertyKey("prop1"), PropertyRef(Reference("x"), avgPropAlias))
            ))),
      setClause = Some(
        SetClause(Seq(
          PropertySet(
            Reference("x"),
            PropAssignment(PropertyKey("prop2"), PropertyRef(Reference("x"), minPropAlias)))
        ))),
      removeClause = Some(
        RemoveClause(
          propRemoves = Seq(
            PropertyRemove(PropertyRef(Reference("x"), avgPropAlias)),
            PropertyRemove(PropertyRef(Reference("x"), minPropAlias))),
          labelRemoves = Seq.empty)
      ))
  }

  test("Create vertex from unbound variable, nested aggregations are passed to the GroupBy - " +
    "CONSTRUCT (x GROUP c.prop3 {prop0 := COUNT(*), prop1 := 1 + COUNT(*)}) " +
    "SET x.prop2 := COUNT(*)") {
    VarBinder.reset()
    val groupingProps = Seq(PropertyRef(Reference("c"), PropertyKey("prop3")))
    val prop0 = PropertyKey("prop0")
    val prop1 = PropertyKey("prop1")
    val prop2 = PropertyKey("prop2")

    val count = Count(distinct = false, Star)
    val onePlusCount = Add(count, IntLiteral(1))

    val aggProp0 = PropertyKey(s"${PROP_AGG_BASENAME}_0")
    val aggProp1 = PropertyKey(s"${PROP_AGG_BASENAME}_1")
    val aggProp2 = PropertyKey(s"${PROP_AGG_BASENAME}_2")

    val algebraTree =
      vertexConstruct(
        reference = Reference("x"),
        groupDeclaration = Some(GroupDeclaration(groupingProps)),
        objConstructPattern =
          ObjectConstructPattern(
            labelAssignments = LabelAssignments(Seq.empty),
            propAssignments =
              PropAssignments(Seq(
                PropAssignment(prop0, count),
                PropAssignment(prop1, onePlusCount)))),
        setClause =
          SetClause(Seq(PropertySet(Reference("x"), PropAssignment(prop2, count)))))

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    actual should matchGroupConstructUnboundGroupedVertex(
      reference = Reference("x"),
      groupingProps = groupingProps,
      aggregateFunctions = Seq(
        PropertySet(Reference("x"), PropAssignment(aggProp0, count)),
        PropertySet(Reference("x"), PropAssignment(aggProp1, onePlusCount)),
        PropertySet(Reference("x"), PropAssignment(aggProp2, count))),
      objectConstructPattern =
        ObjectConstructPattern(
          labelAssignments = LabelAssignments(Seq.empty),
          propAssignments =
            PropAssignments(Seq(
              PropAssignment(prop0, PropertyRef(Reference("x"), aggProp0)),
              PropAssignment(prop1, PropertyRef(Reference("x"), aggProp1))))),
      setClause =
        Some(
          SetClause(Seq(
            PropertySet(
              Reference("x"),
              PropAssignment(prop2, PropertyRef(Reference("x"), aggProp2)))))
        ),
      removeClause =
        Some(
          RemoveClause(
            propRemoves = Seq(
              PropertyRemove(PropertyRef(Reference("x"), aggProp0)),
              PropertyRemove(PropertyRef(Reference("x"), aggProp1)),
              PropertyRemove(PropertyRef(Reference("x"), aggProp2))),
            labelRemoves = Seq.empty)
        )
    )
  }

  test("CreateRules for GroupConstruct of vertex") {
    val algebraTree = vertexConstruct(reference = Reference("c"))
    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    actual shouldBe a [GroupConstruct]
    val createRules = actual.asInstanceOf[GroupConstruct].createRules
    assert(createRules == Seq(VertexCreate(Reference("c"))))
  }

  test("CreateRules for GroupConstruct of edges") {
    val leftEndpoint0 =
      VertexConstruct(
        Reference("left_0"),
        copyPattern = None,
        groupDeclaration = None,
        expr = ObjectConstructPattern.empty)
    val leftEndpoint1 = leftEndpoint0.copy(ref = Reference("left_1"))
    val rightEndpoint0 =
      VertexConstruct(
        Reference("right_0"),
        copyPattern = None,
        groupDeclaration = None,
        expr = ObjectConstructPattern.empty)
    val rightEndpoint1 = rightEndpoint0.copy(ref = Reference("right_1"))
    val edge0 =
      EdgeConstruct(
        connName = Reference("e_0"),
        connType = OutConn,
        leftEndpoint = leftEndpoint0,
        rightEndpoint = rightEndpoint0,
        copyPattern = None,
        groupDeclaration = None,
        expr = ObjectConstructPattern.empty)
    val edge1 =
      edge0.copy(
        connName = Reference("e_1"), leftEndpoint = leftEndpoint1, rightEndpoint = rightEndpoint1)
    val algebraTree = constructClauseWithTopology(topology = Seq(edge0, edge1))

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    actual shouldBe a [GroupConstruct]
    val createRules = actual.asInstanceOf[GroupConstruct].createRules
    assert(
      createRules.toSet ==
        Set(
          VertexCreate(Reference("left_0")), VertexCreate(Reference("right_0")),
          VertexCreate(Reference("left_1")), VertexCreate(Reference("right_1")),
          EdgeCreate(Reference("e_0"), Reference("left_0"), Reference("right_0"), OutConn),
          EdgeCreate(Reference("e_1"), Reference("left_1"), Reference("right_1"), OutConn))
    )
  }

  test("GroupConstruct - unbound ungrouped endpoints only AddColumns to the binding table") {
    val algebraTree =
      edgeConstruct(
        reference = Reference("e_0"), // unmatched
        leftConstruct =
          VertexConstruct(
            ref = Reference("c_0"), // unmatched
            copyPattern = None,
            groupDeclaration = None,
            expr = ObjectConstructPattern.empty),
        rightConstruct =
          VertexConstruct(
            ref = Reference("f_0"), // unmatched
            copyPattern = None,
            groupDeclaration = None,
            expr = ObjectConstructPattern.empty),
        groupDeclaration = None,
        objConstructPattern = ObjectConstructPattern.empty,
        when = True,
        setClause = emptySetClause,
        removeClause = emptyRemoveClause)

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    actual shouldBe a [GroupConstruct]
    val vertexConstructTable = actual.asInstanceOf[GroupConstruct].vertexConstructTable
    val baseConstructViewName = actual.asInstanceOf[GroupConstruct].baseConstructViewName

    inside(vertexConstructTable) {
      case
        ConstructRelation(
          crRef1, /*isMatchedRef =*/ false,
          AddColumn(
            acRef1,
            ConstructRelation(
              crRef2, /*isMatchedRef =*/ false,
              AddColumn(
                acRef2,
                BaseConstructTable(crViewName, _)),
              _, _, _, _)),
          _, _, _, _) =>

        assert(crRef1 == acRef1)
        assert(crRef2 == acRef2)
        assert(Set(crRef1, crRef2) == Set(Reference("c_0"), Reference("f_0")))
        assert(crViewName == baseConstructViewName)
    }
  }

  test("GroupConstruct - bound vertices and unbound grouped vertices are inner-joined with the " +
    "base construct table, if there had been no ungrouped variables") {
    val algebraTree =
      edgeConstruct(
        reference = Reference("e_0"), // unmatched
        leftConstruct =
          VertexConstruct(
            ref = Reference("c"), // matched
            copyPattern = None,
            groupDeclaration = None,
            expr = ObjectConstructPattern.empty),
        rightConstruct =
          VertexConstruct(
            ref = Reference("x"), // unmatched, but grouped
            copyPattern = None,
            groupDeclaration =
              Some(GroupDeclaration(Seq(PropertyRef(Reference("f"), PropertyKey("prop"))))),
            expr = ObjectConstructPattern.empty),
        groupDeclaration = None,
        objConstructPattern = ObjectConstructPattern.empty,
        when = True,
        setClause = emptySetClause,
        removeClause = emptyRemoveClause)

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    actual shouldBe a [GroupConstruct]
    val vertexConstructTable = actual.asInstanceOf[GroupConstruct].vertexConstructTable

    inside(vertexConstructTable) {
      case
        InnerJoin(
          InnerJoin(BaseConstructTable(_, _), relation1, _),
          relation2,
          _) =>

        val vertexRefs: mutable.Set[Reference] = mutable.Set[Reference]()
        relation1.forEachDown {
          case cr: ConstructRelation => vertexRefs += cr.reference
          case _ =>
        }
        relation2.forEachDown {
          case cr: ConstructRelation => vertexRefs += cr.reference
          case _ =>
        }
        assert(vertexRefs == Set(Reference("c"), Reference("x")))
    }
  }

  test("GroupConstruct - grouped vertices are inner joined with the binding table with ungrouped " +
    "vertices") {
    val algebraTree =
      edgeConstruct(
        reference = Reference("e_0"), // unmatched
        leftConstruct =
          VertexConstruct(
            ref = Reference("c_0"), // unmatched
            copyPattern = None,
            groupDeclaration = None,
            expr = ObjectConstructPattern.empty),
        rightConstruct =
          VertexConstruct(
            ref = Reference("f"), // matched (grouped by identity)
            copyPattern = None,
            groupDeclaration = None,
            expr = ObjectConstructPattern.empty),
        groupDeclaration = None,
        objConstructPattern = ObjectConstructPattern.empty,
        when = True,
        setClause = emptySetClause,
        removeClause = emptyRemoveClause)

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    actual shouldBe a [GroupConstruct]
    val vertexConstructTable = actual.asInstanceOf[GroupConstruct].vertexConstructTable

    inside(vertexConstructTable) {
      case
        InnerJoin(
          ConstructRelation(Reference("c_0"), /*isMatchedRef =*/ false,  _, _, _, _, _),
          Project(
            ConstructRelation(Reference("f"), /*isMatchedRef =*/ true, _, _, _, _, _),
            _),
          _) =>
    }
  }

  test("GroupConstruct - endpoints are only constructed once, even if they appear multiple times " +
    "in the construct pattern - CONSTRUCT (c)->(f)->(c)") {
    val c =
      VertexConstruct(
        Reference("c"),
        copyPattern = None,
        groupDeclaration = None,
        expr = ObjectConstructPattern.empty)
    val f = c.copy(ref = Reference("f"))
    val edge0 =
      EdgeConstruct(
        connName = Reference("e_0"),
        connType = OutConn,
        leftEndpoint = c,
        rightEndpoint = f,
        copyPattern = None,
        groupDeclaration = None,
        expr = ObjectConstructPattern.empty)
    val edge1 = edge0.copy(connName = Reference("e_1"), leftEndpoint = f, rightEndpoint = c)
    val algebraTree = constructClauseWithTopology(topology = Seq(edge0, edge1))

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    actual shouldBe a [GroupConstruct]
    val vertexConstructTable = actual.asInstanceOf[GroupConstruct].vertexConstructTable

    val expectedVertexRefs: Set[Reference] = Set(Reference("c"), Reference("f"))
    val actualVertexRefs: mutable.ArrayBuffer[Reference] = mutable.ArrayBuffer[Reference]()
    vertexConstructTable.forEachDown {
      case cr: ConstructRelation => actualVertexRefs += cr.reference
      case _ =>
    }

    assert(actualVertexRefs.size == expectedVertexRefs.size)
    assert(actualVertexRefs.toSet == expectedVertexRefs)
  }

  test("GroupConstruct - GROUP-ing for a vertex is detected anywhere in the pattern - " +
    "CONSTRUCT (y)->(x GROUP f.prop)->(f_0)->(x)->(y GROUP c.prop)") {
    val xNoGrouping =
      VertexConstruct(
        Reference("x"),
        copyPattern = None,
        groupDeclaration = None,
        expr = ObjectConstructPattern.empty)
    val xGrouping =
      xNoGrouping.copy(
        groupDeclaration =
          Some(GroupDeclaration(Seq(PropertyRef(Reference("f"), PropertyKey("prop")))))
      )
    val yNoGrouping =
      VertexConstruct(
        Reference("y"),
        copyPattern = None,
        groupDeclaration = None,
        expr = ObjectConstructPattern.empty)
    val yGrouping =
      yNoGrouping.copy(
        groupDeclaration =
          Some(GroupDeclaration(Seq(PropertyRef(Reference("c"), PropertyKey("prop")))))
      )
    val f0 = xNoGrouping.copy(ref = Reference("f_0")) // unmatched => only AddColumn
    // (y) -> (x GROUP ..)
    val edge0 =
      EdgeConstruct(
        connName = Reference("e_0"),
        connType = OutConn,
        leftEndpoint = yNoGrouping,
        rightEndpoint = xGrouping,
        copyPattern = None,
        groupDeclaration = None,
        expr = ObjectConstructPattern.empty)
    // (x GROUP ..) -> (f0)
    val edge1 =
      edge0.copy(connName = Reference("e_1"), leftEndpoint = xGrouping, rightEndpoint = f0)
    // (f0) -> (x)
    val edge2 =
      edge0.copy(connName = Reference("e_2"), leftEndpoint = f0, rightEndpoint = xNoGrouping)
    // (x) -> (y GROUP ..)
    val edge3 =
      edge0.copy(connName = Reference("e_3"), leftEndpoint = xNoGrouping, rightEndpoint = yGrouping)
    val algebraTree = constructClauseWithTopology(topology = Seq(edge0, edge1, edge2, edge3))

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    actual shouldBe a [GroupConstruct]
    val vertexConstructTable = actual.asInstanceOf[GroupConstruct].vertexConstructTable

    inside(vertexConstructTable) {
      case
        InnerJoin(
          InnerJoin(
            ConstructRelation(Reference("f_0"), /*isMatchedRef =*/ false,  _, _, _, _, _),
            ConstructRelation(groupedRef1, /*isMatchedRef =*/ false, _, _, _, _, _), _),
          ConstructRelation(groupedRef2, /*isMatchedRef =*/ false, _, _, _, _, _), _) =>

        val expectedGroupedVertexRefs = Set(Reference("x"), Reference("y"))
        val actualGroupedVertexRefs = Set(groupedRef1, groupedRef2)
        assert(actualGroupedVertexRefs == expectedGroupedVertexRefs)
    }
  }

  test("GroupConstruct - edge is inner-joined with the VertexConstructTable view") {
    val algebraTree =
      edgeConstruct(
        reference = Reference("e"), // matched
        leftConstruct =
          VertexConstruct(
            ref = Reference("c"), // matched
            copyPattern = None,
            groupDeclaration = None,
            expr = ObjectConstructPattern.empty),
        rightConstruct =
          VertexConstruct(
            ref = Reference("f"), // matched
            copyPattern = None,
            groupDeclaration = None,
            expr = ObjectConstructPattern.empty),
        groupDeclaration = None,
        objConstructPattern = ObjectConstructPattern.empty,
        when = True,
        setClause = emptySetClause,
        removeClause = emptyRemoveClause)

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    actual shouldBe a [GroupConstruct]
    val edgeConstructTable = actual.asInstanceOf[GroupConstruct].edgeConstructTable
    val vertexConstructViewName = actual.asInstanceOf[GroupConstruct].vertexConstructViewName

    edgeConstructTable should matchPattern {
      case InnerJoin(VertexConstructTable(`vertexConstructViewName`, _), _, _) =>
    }
  }

  test("GroupConstruct - bound edge is grouped-by its endpoints on the VertexConstructTable view") {
    val algebraTree =
      edgeConstruct(
        reference = Reference("e"), // matched
        leftConstruct =
          VertexConstruct(
            ref = Reference("c"), // matched
            copyPattern = None,
            groupDeclaration = None,
            expr = ObjectConstructPattern.empty),
        rightConstruct =
          VertexConstruct(
            ref = Reference("f"), // matched
            copyPattern = None,
            groupDeclaration = None,
            expr = ObjectConstructPattern.empty),
        groupDeclaration = None,
        objConstructPattern = ObjectConstructPattern.empty,
        when = True,
        setClause = emptySetClause,
        removeClause = emptyRemoveClause)

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    actual shouldBe a [GroupConstruct]
    val edgeConstructTable = actual.asInstanceOf[GroupConstruct].edgeConstructTable

    inside(edgeConstructTable) {
      case InnerJoin(
        _,
        Project(
          ConstructRelation(
            Reference("e"), /*isMatchedRef =*/ true,
            GroupBy(
              Reference("e"),
              VertexConstructTable(_, _),
              groupingAttrs,
              _, _),
            _, _, _, _),
          projectAttrs), _) =>

        val expectedGroupingAttrs = Set(Reference("c"), Reference("f"))
        val expectedProjectAttrs = Set(Reference("c"), Reference("f"), Reference("e"))

        assert(groupingAttrs.size == expectedGroupingAttrs.size)
        assert(groupingAttrs.toSet == expectedGroupingAttrs)
        assert(projectAttrs == expectedProjectAttrs)
    }
  }

  test("GroupConstruct - unbound edge is grouped-by its endpoints on the VertexConstructTable " +
    "view") {
    val algebraTree =
      edgeConstruct(
        reference = Reference("e_0"), // unmatched
        leftConstruct =
          VertexConstruct(
            ref = Reference("c"), // matched
            copyPattern = None,
            groupDeclaration = None,
            expr = ObjectConstructPattern.empty),
        rightConstruct =
          VertexConstruct(
            ref = Reference("f"), // matched
            copyPattern = None,
            groupDeclaration = None,
            expr = ObjectConstructPattern.empty),
        groupDeclaration = None,
        objConstructPattern = ObjectConstructPattern.empty,
        when = True,
        setClause = emptySetClause,
        removeClause = emptyRemoveClause)

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    actual shouldBe a [GroupConstruct]
    val edgeConstructTable = actual.asInstanceOf[GroupConstruct].edgeConstructTable

    inside(edgeConstructTable) {
      case InnerJoin(
        _,
        Project(
          ConstructRelation(
            Reference("e_0"), /*isMatchedRef =*/ false,
            AddColumn(
              Reference("e_0"),
              GroupBy(
                Reference("e_0"),
                VertexConstructTable(_, _),
                groupingAttrs,
                _, _)
            ),
            _, _, _, _),
          projectAttrs), _) =>

        val expectedGroupingAttrs = Set(Reference("c"), Reference("f"))
        val expectedProjectAttrs = Set(Reference("c"), Reference("f"), Reference("e_0"))

        assert(groupingAttrs.size == expectedGroupingAttrs.size)
        assert(groupingAttrs.toSet == expectedGroupingAttrs)
        assert(projectAttrs == expectedProjectAttrs)
    }
  }

  private def vertexConstruct(reference: Reference,
                              groupDeclaration: Option[GroupDeclaration] = None,
                              objConstructPattern: ObjectConstructPattern =
                                ObjectConstructPattern.empty,
                              when: AlgebraExpression = True,
                              setClause: SetClause = emptySetClause,
                              removeClause: RemoveClause = emptyRemoveClause): ConstructClause = {
    constructClauseWithTopology(
      topology = Seq(
        VertexConstruct(
          reference,
          copyPattern = None,
          groupDeclaration,
          expr = objConstructPattern)),
      when, setClause, removeClause)
  }

  private def edgeConstruct(reference: Reference,
                            leftConstruct: VertexConstruct,
                            rightConstruct: VertexConstruct,
                            groupDeclaration: Option[GroupDeclaration] = None,
                            objConstructPattern: ObjectConstructPattern =
                              ObjectConstructPattern.empty,
                            when: AlgebraExpression = True,
                            setClause: SetClause = emptySetClause,
                            removeClause: RemoveClause = emptyRemoveClause): ConstructClause = {
    constructClauseWithTopology(
      topology = Seq(
        EdgeConstruct(
          connName = reference,
          connType = OutConn,
          leftEndpoint = leftConstruct,
          rightEndpoint = rightConstruct,
          copyPattern = None,
          groupDeclaration,
          expr = objConstructPattern)),
      when, setClause, removeClause)
  }

  private def constructClauseWithTopology(topology: Seq[ConnectionConstruct],
                                          when: AlgebraExpression = True,
                                          setClause: SetClause = emptySetClause,
                                          removeClause: RemoveClause = emptyRemoveClause)
  : ConstructClause = {

    ConstructClause(
      graphs = GraphUnion(Seq.empty),
      CondConstructClause(
        condConstructs = Seq(
          BasicConstructClause(
            ConstructPattern(topology),
            when
          ))
      ),
      setClause, removeClause)
  }

  private def extractConstructRelations(constructTree: AlgebraTreeNode,
                                        expectedNumConstructs: Int = 1): Seq[AlgebraTreeNode] = {
    val condConstructClause = constructTree.children(1)
    val constructRelations = condConstructClause.children
    assert(constructRelations.size == expectedNumConstructs)
    constructRelations
  }
}
