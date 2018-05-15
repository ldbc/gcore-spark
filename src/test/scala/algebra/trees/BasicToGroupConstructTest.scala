package algebra.trees

import algebra.expressions._
import algebra.operators._
import algebra.trees.BasicToGroupConstruct.PROP_AGG_BASENAME
import algebra.trees.CustomMatchers._
import algebra.types._
import org.scalatest.{FunSuite, Inside, Matchers}
import parser.utils.VarBinder
import schema.Catalog

import scala.collection.mutable

class BasicToGroupConstructTest extends FunSuite with Matchers with Inside {

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

  private val basicToGroupConstructRewriter: BasicToGroupConstruct =
    BasicToGroupConstruct(
      AlgebraContext(Catalog.empty, bindingToGraph = None, bindingContext = Some(bindingContext)))

  private val emptySetClause: SetClause = SetClause(Seq.empty)
  private val emptyRemoveClause: RemoveClause =
    RemoveClause(labelRemoves = Seq.empty, propRemoves = Seq.empty)

  test("There are as many GroupConstruct as BasicConstructClause") {
    val vertex1 = vertexConstruct(Reference("v1"))
    val vertex2 = vertexConstruct(Reference("v2"))
    val algebraTree = constructClause(topologies = Seq(Seq(vertex1), Seq(vertex2)))
    extractConstructRelations(
      constructTree = rewrite(algebraTree),
      expectedNumConstructs = 2)
  }

  test("Create vertex from bound variable - CONSTRUCT (c)") {
    val algebraTree = constructClauseVertex(reference = Reference("c"))
    val actual = extractConstructRelations(rewrite(algebraTree)).head
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
      constructClauseVertex(Reference("c"), objConstructPattern = objectConstructPattern)

    val actual = extractConstructRelations(rewrite(algebraTree)).head
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
      constructClauseVertex(Reference("c"), setClause = SetClause(Seq(prop1, prop2, prop3)))

    val actual = extractConstructRelations(rewrite(algebraTree)).head
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
      constructClauseVertex(
        reference = Reference("c"),
        removeClause =
          RemoveClause(
            propRemoves = Seq(prop1, prop2),
            labelRemoves = Seq(label1, label2))
      )

    val actual = extractConstructRelations(rewrite(algebraTree)).head
    actual should matchGroupConstructBoundVertex(
      reference = Reference("c"),
      removeClause = Some(RemoveClause(propRemoves = Seq(prop1), labelRemoves = Seq(label1)))
    )
  }

  test("Create vertex from bound variable, WHEN condition filters the binding table - " +
    "CONSTRUCT (c) WHEN f.prop1 > 2") {
    val condition = Gt(PropertyRef(Reference("f"), PropertyKey("prop1")), IntLiteral(2))
    val algebraTree = constructClauseVertex(reference = Reference("c"), when = condition)
    val actual = extractConstructRelations(rewrite(algebraTree)).head
    actual should matchGroupConstructBoundVertex(
      reference = Reference("c"),
      when = condition)
  }

  test("Create vertex from unbound variable - CONSTRUCT (x)") {
    val algebraTree = constructClauseVertex(reference = Reference("x"))
    val actual = extractConstructRelations(rewrite(algebraTree)).head
    actual should matchGroupConstructUnboundVertex(reference = Reference("x"))
  }

  test("Create vertex from unbound variable, GROUPing attributes are passed to GroupBy - " +
    "CONSTRUCT (x GROUP c.prop1)") {
    val groupingProps = Seq(PropertyRef(Reference("c"), PropertyKey("prop1")))
    val algebraTree =
      constructClauseVertex(
        reference = Reference("x"),
        groupDeclaration = Some(GroupDeclaration(groupingProps)))

    val actual = extractConstructRelations(rewrite(algebraTree)).head
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
      constructClauseVertex(
        reference = Reference("x"),
        objConstructPattern = objConstructPattern,
        setClause = setClause)

    val actual = extractConstructRelations(rewrite(algebraTree)).head
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
      constructClauseVertex(
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
    val actual = extractConstructRelations(rewrite(algebraTree)).head
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
      aggPropRemoveClause = Some(
        RemoveClause(
          propRemoves = Seq(
            PropertyRemove(PropertyRef(Reference("x"), avgPropAlias)),
            PropertyRemove(PropertyRef(Reference("x"), minPropAlias))),
          labelRemoves = Seq.empty)
      ))
  }

  test("Create vertex from unbound variable, aggregation sub-trees are passed to the GroupBy - " +
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
      constructClauseVertex(
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

    val actual = extractConstructRelations(rewrite(algebraTree)).head
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
      aggPropRemoveClause =
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
    val algebraTree = constructClauseVertex(reference = Reference("c"))
    val actual = extractConstructRelations(rewrite(algebraTree)).head
    actual shouldBe a [GroupConstruct]
    val createRules = actual.asInstanceOf[GroupConstruct].createRules
    assert(createRules == Seq(VertexCreate(Reference("c"), removeClause = None)))
  }

  test("CreateRules for GroupConstruct of edges") {
    val leftEndpoint0 = vertexConstruct(Reference("left_0"))
    val leftEndpoint1 = vertexConstruct(Reference("left_1"))
    val rightEndpoint0 = vertexConstruct(Reference("right_0"))
    val rightEndpoint1 = vertexConstruct(Reference("right_1"))
    val edge0 = edgeConstruct(Reference("e_0"), leftEndpoint0, rightEndpoint0)
    val edge1 = edgeConstruct(Reference("e_1"), leftEndpoint1, rightEndpoint1)
    val algebraTree = constructClause(topologies = Seq(Seq(edge0, edge1)))

    val actual = extractConstructRelations(rewrite(algebraTree)).head
    actual shouldBe a [GroupConstruct]
    val createRules = actual.asInstanceOf[GroupConstruct].createRules
    assert(
      createRules.toSet ==
        Set(
          VertexCreate(Reference("left_0"), removeClause = None),
          VertexCreate(Reference("right_0"), removeClause = None),
          VertexCreate(Reference("left_1"), removeClause = None),
          VertexCreate(Reference("right_1"), removeClause = None),
          EdgeCreate(
            Reference("e_0"), Reference("left_0"), Reference("right_0"), OutConn,
            removeClause = None),
          EdgeCreate(
            Reference("e_1"), Reference("left_1"), Reference("right_1"), OutConn,
            removeClause = None))
    )
  }

  test("GroupConstruct - unbound ungrouped endpoints only AddColumns to the binding table") {
    val algebraTree =
      constructClauseEdge(
        reference = Reference("e_0"), // unmatched
        leftConstruct = vertexConstruct(Reference("c_0")), // unmatched
        rightConstruct = vertexConstruct(Reference("f_0"))) // unmatched

    val actual = extractConstructRelations(rewrite(algebraTree)).head
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
      constructClauseEdge(
        reference = Reference("e_0"), // unmatched
        leftConstruct = vertexConstruct(Reference("c")), // matched
        rightConstruct =
          vertexConstruct(
            Reference("x"), // unmatched, but grouped
            groupDeclaration =
              Some(GroupDeclaration(Seq(PropertyRef(Reference("f"), PropertyKey("prop"))))))
      )

    val actual = extractConstructRelations(rewrite(algebraTree)).head
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
      constructClauseEdge(
        reference = Reference("e_0"), // unmatched
        leftConstruct = vertexConstruct(Reference("c_0")), // unmatched
        rightConstruct = vertexConstruct(Reference("f"))) // matched (grouped by identity)

    val actual = extractConstructRelations(rewrite(algebraTree)).head
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
    val c = vertexConstruct(Reference("c"))
    val f = vertexConstruct(Reference("f"))
    val edge0 = edgeConstruct(edgeReference = Reference("e_0"), leftEndpoint = c, rightEndpoint = f)
    val edge1 = edgeConstruct(edgeReference = Reference("e_1"), leftEndpoint = f, rightEndpoint = c)
    val algebraTree = constructClause(topologies = Seq(Seq(edge0, edge1)))

    val actual = extractConstructRelations(rewrite(algebraTree)).head
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
    val xNoGrouping = vertexConstruct(Reference("x"))
    val xGrouping =
      vertexConstruct(
        Reference("x"),
        groupDeclaration =
          Some(GroupDeclaration(Seq(PropertyRef(Reference("f"), PropertyKey("prop")))))
      )
    val yNoGrouping = vertexConstruct(Reference("y"))
    val yGrouping =
      vertexConstruct(
        Reference("y"),
        groupDeclaration =
          Some(GroupDeclaration(Seq(PropertyRef(Reference("c"), PropertyKey("prop")))))
      )
    val f0 = vertexConstruct(Reference("f_0")) // unmatched => only AddColumn
    // (y) -> (x GROUP ..)
    val edge0 =
      edgeConstruct(
        edgeReference = Reference("e_0"), leftEndpoint = yNoGrouping, rightEndpoint = xGrouping)
    // (x GROUP ..) -> (f0)
    val edge1 =
      edgeConstruct(edgeReference = Reference("e_1"), leftEndpoint = xGrouping, rightEndpoint = f0)
    // (f0) -> (x)
    val edge2 =
      edgeConstruct(
        edgeReference = Reference("e_2"), leftEndpoint = f0, rightEndpoint = xNoGrouping)
    // (x) -> (y GROUP ..)
    val edge3 =
      edgeConstruct(
        edgeReference = Reference("e_3"), leftEndpoint = xNoGrouping, rightEndpoint = yGrouping)
    val algebraTree = constructClause(topologies = Seq(Seq(edge0, edge1, edge2, edge3)))

    val actual = extractConstructRelations(rewrite(algebraTree)).head
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
      constructClauseEdge(
        reference = Reference("e"), // matched
        leftConstruct = vertexConstruct(Reference("c")), // matched
        rightConstruct = vertexConstruct(Reference("f"))) // matched

    val actual = extractConstructRelations(rewrite(algebraTree)).head
    actual shouldBe a [GroupConstruct]
    val edgeConstructTable = actual.asInstanceOf[GroupConstruct].edgeConstructTable
    val vertexConstructViewName = actual.asInstanceOf[GroupConstruct].vertexConstructViewName

    edgeConstructTable should matchPattern {
      case InnerJoin(VertexConstructTable(`vertexConstructViewName`, _), _, _) =>
    }
  }

  test("GroupConstruct - bound edge is grouped-by its endpoints on the VertexConstructTable view") {
    val algebraTree =
      constructClauseEdge(
        reference = Reference("e"), // matched
        leftConstruct = vertexConstruct(Reference("c")), // matched
        rightConstruct = vertexConstruct(Reference("f"))) // matched

    val actual = extractConstructRelations(rewrite(algebraTree)).head
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
      constructClauseEdge(
        reference = Reference("e_0"), // unmatched
        leftConstruct = vertexConstruct(Reference("c")), // matched
        rightConstruct = vertexConstruct(Reference("f"))) // matched

    val actual = extractConstructRelations(rewrite(algebraTree)).head
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

  test("GroupConstruct created correctly after BasicConstructs normalization - " +
    "CONSTRUCT (v), (v)-[e]->(w)") {
    val v = vertexConstruct(Reference("v"))
    val w = vertexConstruct(Reference("w"))
    val e = edgeConstruct(Reference("e"), v, w)
    val algebraTree = constructClause(topologies = Seq(Seq(v), Seq(e)))

    val actual = extractConstructRelations(rewrite(algebraTree)).head
    inside(actual) {
      case
        GroupConstruct(
          /*baseConstructTable =*/ _,
          /*vertexConstructTable =*/ ConstructRelation(
            vtableRef1, /*isMatchedRef =*/ _,
            AddColumn(
              /*reference =*/ _,
              ConstructRelation(
                vtableRef2, /*isMatchedRef =*/ _, _, _, _, _, _)
            ), _, _, _, _),
          /*baseConstructViewName =*/ _, /*vertexConstructViewName =*/ _,
          /*edgeConstructTable =*/ InnerJoin(
            _,
            Project(ConstructRelation(Reference("e"), _, _, _, _, _, _), _),
            _),
          /*createRules =*/ _) =>
        assert(Set(vtableRef1, vtableRef2) == Set(Reference("v"), Reference("w")))
    }
  }

  test("VertexConstruct properties are merged for all vertex occurrences in GroupConstruct - " +
    "CONSTRUCT (a {prop0 := 0}), (a {prop1 := 1})") {
    val prop0 = PropAssignment(PropertyKey("prop0"), IntLiteral(0))
    val prop1 = PropAssignment(PropertyKey("prop1"), IntLiteral(1))
    val aprop0 =
      vertexConstruct(
        reference = Reference("a"),
        objConstructPattern =
          ObjectConstructPattern(
            propAssignments = PropAssignments(Seq(prop0)),
            labelAssignments = LabelAssignments(Seq.empty))
      )
    val aprop1 =
      vertexConstruct(
        reference = Reference("a"),
        objConstructPattern =
          ObjectConstructPattern(
            propAssignments = PropAssignments(Seq(prop1)),
            labelAssignments = LabelAssignments(Seq.empty))
      )
    val algebraTree = constructClause(topologies = Seq(Seq(aprop0), Seq(aprop1)))
    val actual = extractConstructRelations(rewrite(algebraTree)).head
    actual should matchGroupConstructUnboundVertex(
      reference = Reference("a"),
      objectConstructPattern =
        ObjectConstructPattern(
          propAssignments = PropAssignments(Seq(prop0, prop1)),
          labelAssignments = LabelAssignments(Seq.empty))
    )
  }

  test("VertexConstruct GROUP-ings are merged for all vertex occurrences in GroupConstruct - " +
    "CONSTRUCT (a GROUP c.prop0), (a GROUP c.prop1)") {
    val prop0 = PropertyRef(Reference("c"), PropertyKey("prop0"))
    val prop1 = PropertyRef(Reference("c"), PropertyKey("prop1"))
    val aprop0 =
      vertexConstruct(
        reference = Reference("a"),
        groupDeclaration = Some(GroupDeclaration(Seq(prop0)))
      )
    val aprop1 =
      vertexConstruct(
        reference = Reference("a"),
        groupDeclaration = Some(GroupDeclaration(Seq(prop1)))
      )
    val algebraTree = constructClause(topologies = Seq(Seq(aprop0), Seq(aprop1)))
    val actual = extractConstructRelations(rewrite(algebraTree)).head
    actual should matchGroupConstructUnboundGroupedVertex(
      reference = Reference("a"),
      groupingProps = Seq(prop0, prop1)
    )
  }

  test("EdgeConstruct properties are merged for all edge occurrences in GroupConstruct - " +
    "CONSTRUCT (a)-[e]->(b), (a)-[e {prop := 0}]->(b)") {
    val prop = PropAssignment(PropertyKey("prop0"), IntLiteral(0))
    val a = vertexConstruct(Reference("a"))
    val b = vertexConstruct(Reference("b"))
    val e = edgeConstruct(Reference("e"), a, b)
    val eprop =
      edgeConstruct(
        Reference("e"), a, b,
        objConstructPattern =
          ObjectConstructPattern(
            labelAssignments = LabelAssignments(Seq.empty),
            propAssignments = PropAssignments(Seq(prop)))
      )
    val algebraTree = constructClause(topologies = Seq(Seq(e), Seq(eprop)))
    val actual = extractConstructRelations(rewrite(algebraTree)).head
    val edgeConstructTable = actual.asInstanceOf[GroupConstruct].edgeConstructTable

    edgeConstructTable should matchPattern {
      case
        InnerJoin(
          _,
          Project(
            ConstructRelation(
              Reference("e"), /*isMatchedRef =*/ true,
              /*relation =*/ _, /*groupedAtts =*/ _,
              ObjectConstructPattern(
                LabelAssignments(Seq()),
                PropAssignments(Seq(`prop`))),
              _, _),
          /*projectAttrs =*/ _), _) =>
    }
  }

  private def rewrite(tree: AlgebraTreeNode): AlgebraTreeNode = {
    basicToGroupConstructRewriter.rewriteTree(
      NormalizeBasicConstructs.rewriteTree(tree))
  }

  private def constructClauseVertex(reference: Reference,
                                    copyPattern: Option[Reference] = None,
                                    groupDeclaration: Option[GroupDeclaration] = None,
                                    objConstructPattern: ObjectConstructPattern =
                                      ObjectConstructPattern.empty,
                                    when: AlgebraExpression = True,
                                    setClause: SetClause = emptySetClause,
                                    removeClause: RemoveClause = emptyRemoveClause)
  : ConstructClause = {

    constructClause(
      topologies = Seq(
        Seq(vertexConstruct(reference, copyPattern, groupDeclaration, objConstructPattern))),
      when, setClause, removeClause)
  }

  private def constructClauseEdge(reference: Reference,
                                  leftConstruct: VertexConstruct,
                                  rightConstruct: VertexConstruct,
                                  groupDeclaration: Option[GroupDeclaration] = None,
                                  objConstructPattern: ObjectConstructPattern =
                                    ObjectConstructPattern.empty,
                                  when: AlgebraExpression = True,
                                  setClause: SetClause = emptySetClause,
                                  removeClause: RemoveClause = emptyRemoveClause)
  : ConstructClause = {

    constructClause(
      topologies = Seq(
        Seq(
          edgeConstruct(
            reference, leftConstruct, rightConstruct, groupDeclaration, objConstructPattern))),
      when, setClause, removeClause)
  }

  private def vertexConstruct(reference: Reference,
                              copyPattern: Option[Reference] = None,
                              groupDeclaration: Option[GroupDeclaration] = None,
                              objConstructPattern: ObjectConstructPattern =
                                ObjectConstructPattern.empty): VertexConstruct = {
    VertexConstruct(reference, copyPattern, groupDeclaration, objConstructPattern)
  }

  private def edgeConstruct(edgeReference: Reference,
                            leftEndpoint: VertexConstruct,
                            rightEndpoint: VertexConstruct,
                            groupDeclaration: Option[GroupDeclaration] = None,
                            objConstructPattern: ObjectConstructPattern =
                              ObjectConstructPattern.empty): EdgeConstruct = {
    EdgeConstruct(
      connName = edgeReference,
      connType = OutConn,
      leftEndpoint = leftEndpoint,
      rightEndpoint = rightEndpoint,
      copyPattern = None,
      groupDeclaration = groupDeclaration,
      expr = objConstructPattern)
  }

  private def constructClause(topologies: Seq[Seq[ConnectionConstruct]],
                              when: AlgebraExpression = True,
                              setClause: SetClause = emptySetClause,
                              removeClause: RemoveClause = emptyRemoveClause): ConstructClause = {
    ConstructClause(
      graphs = GraphUnion(Seq.empty),
      CondConstructClause(
        topologies.map(connectionConstruct =>
          BasicConstructClause(ConstructPattern(connectionConstruct), when))
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
