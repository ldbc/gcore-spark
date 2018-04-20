package algebra.trees

import algebra.expressions._
import algebra.operators._
import algebra.trees.CreateGroupingSets.PROP_AGG_BASENAME
import algebra.types.{ConstructPattern, GroupDeclaration, VertexConstruct}
import org.scalatest.{FunSuite, Inside, Matchers}
import parser.utils.VarBinder
import schema.GraphDb

class CreateGroupingSetsTest extends FunSuite with Inside with Matchers {

  /**
    *                 +---+
    *                 | c |
    * bindingTable =  +---+
    *                 |...|
    */
  private val bindingTable: BindingTable = BindingTable(new BindingSet(Reference("c")))

  private val bindingContext: BindingContext =
    BindingContext(
      vertexBindings = Set(Reference("c")),
      edgeBindings = Set.empty,
      pathBindings = Set.empty)

  private val rewriter: CreateGroupingSets =
    CreateGroupingSets(
      AlgebraContext(GraphDb.empty, bindingToGraph = None, bindingContext = Some(bindingContext)))

  private val emptyObjConstruct: ObjectConstructPattern =
    ObjectConstructPattern(
      propAssignments = PropAssignments(Seq.empty),
      labelAssignments = LabelAssignments(Seq.empty))
  private val emptySetClause: SetClause = SetClause(Seq.empty)
  private val emptyRemoveClause: RemoveClause =
    RemoveClause(labelRemoves = Seq.empty, propRemoves = Seq.empty)

  test("Create vertex from bound variable - CONSTRUCT (c)") {
    val algebraTree =
      vertexConstruct(
        reference = Reference("c"),
        groupDeclaration = None,
        objConstructPattern = emptyObjConstruct,
        when = True,
        setClause = emptySetClause,
        removeClause = emptyRemoveClause)

    val expected =
      VertexConstructRelation(
        Reference("c"),
        relation =
          Project(
            relation =
              GroupBy(
                reference = Reference("c"),
                relation = Select(bindingTable, True),
                groupingAttributes = Seq(Reference("c")),
                aggregateFunctions = Seq.empty,
                having = None),
            attributes = Set(Reference("c"))),
        expr = emptyObjConstruct,
        setClause = None,
        removeClause = None)

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    assert(actual == expected)
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
      vertexConstruct(
        reference = Reference("c"),
        groupDeclaration = None,
        objConstructPattern = objectConstructPattern,
        when = True,
        setClause = emptySetClause,
        removeClause = emptyRemoveClause)

    val expected =
      VertexConstructRelation(
        Reference("c"),
        relation =
          Project(
            relation =
              GroupBy(
                reference = Reference("c"),
                relation = Select(bindingTable, True),
                groupingAttributes = Seq(Reference("c")),
                aggregateFunctions = Seq.empty,
                having = None),
            attributes = Set(Reference("c"))),
        expr = objectConstructPattern,
        setClause = None,
        removeClause = None)

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    assert(actual == expected)
  }

  test("Create vertex from bound variable, only properties SET for (c) are passed to relation - " +
    "CONSTRUCT (c) SET c.prop1 := 1 SET f.prop2 := 2 SET c.prop3 := 3") {
    val prop1 = PropertySet(Reference("c"), PropAssignment(PropertyKey("prop1"), IntLiteral(1)))
    val prop2 = PropertySet(Reference("c"), PropAssignment(PropertyKey("prop2"), IntLiteral(2)))
    val prop3 = PropertySet(Reference("f"), PropAssignment(PropertyKey("prop1"), IntLiteral(3)))
    val algebraTree =
      vertexConstruct(
        reference = Reference("c"),
        groupDeclaration = None,
        objConstructPattern = emptyObjConstruct,
        when = True,
        setClause = SetClause(Seq(prop1, prop2, prop3)),
        removeClause = emptyRemoveClause)

    val expected =
      VertexConstructRelation(
        Reference("c"),
        relation =
          Project(
            relation =
              GroupBy(
                reference = Reference("c"),
                relation = Select(bindingTable, True),
                groupingAttributes = Seq(Reference("c")),
                aggregateFunctions = Seq.empty,
                having = None),
            attributes = Set(Reference("c"))),
        expr = emptyObjConstruct,
        setClause = Some(SetClause(Seq(prop1, prop2))),
        removeClause = None)

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    assert(actual == expected)
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
        groupDeclaration = None,
        objConstructPattern = emptyObjConstruct,
        when = True,
        setClause = emptySetClause,
        removeClause =
          RemoveClause(
            propRemoves = Seq(prop1, prop2),
            labelRemoves = Seq(label1, label2))
      )

    val expected =
      VertexConstructRelation(
        Reference("c"),
        relation =
          Project(
            relation =
              GroupBy(
                reference = Reference("c"),
                relation = Select(bindingTable, True),
                groupingAttributes = Seq(Reference("c")),
                aggregateFunctions = Seq.empty,
                having = None),
            attributes = Set(Reference("c"))),
        expr = emptyObjConstruct,
        setClause = None,
        removeClause = Some(
          RemoveClause(
            propRemoves = Seq(prop1),
            labelRemoves = Seq(label1))
        ))

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    assert(actual == expected)
  }

  test("Create vertex from bound variable, WHEN condition filters the binding table - " +
    "CONSTRUCT (c) WHEN f.prop1 > 2") {
    val condition = Gt(PropertyRef(Reference("f"), PropertyKey("prop1")), IntLiteral(2))
    val algebraTree =
      vertexConstruct(
        reference = Reference("c"),
        groupDeclaration = None,
        objConstructPattern = emptyObjConstruct,
        when = condition,
        setClause = emptySetClause,
        removeClause = emptyRemoveClause)

    val expected =
      VertexConstructRelation(
        Reference("c"),
        relation =
          Project(
            relation =
              GroupBy(
                reference = Reference("c"),
                relation = Select(bindingTable, condition),
                groupingAttributes = Seq(Reference("c")),
                aggregateFunctions = Seq.empty,
                having = None),
            attributes = Set(Reference("c"))),
        expr = emptyObjConstruct,
        setClause = None,
        removeClause = None)

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    assert(actual == expected)
  }

  test("Create vertex from unbound variable - CONSTRUCT (x)") {
    val algebraTree =
      vertexConstruct(
        reference = Reference("x"),
        groupDeclaration = None,
        objConstructPattern = emptyObjConstruct,
        when = True,
        setClause = emptySetClause,
        removeClause = emptyRemoveClause)

    val expected =
      VertexConstructRelation(
        Reference("x"),
        relation =
          Project(
            relation =
              GroupBy(
                reference = Reference("x"),
                relation = Select(bindingTable, True),
                groupingAttributes = Seq.empty,
                aggregateFunctions = Seq.empty,
                having = None),
            attributes = Set(Reference("x"))),
        expr = emptyObjConstruct,
        setClause = None,
        removeClause = None)

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    assert(actual == expected)
  }

  test("Create vertex from unbound variable, GROUPing attributes are passed to GroupBy - " +
    "CONSTRUCT (x GROUP c.prop1)") {
    val groupDeclaration = GroupDeclaration(Seq(PropertyRef(Reference("c"), PropertyKey("prop1"))))
    val algebraTree =
      vertexConstruct(
        reference = Reference("x"),
        groupDeclaration = Some(groupDeclaration),
        objConstructPattern = emptyObjConstruct,
        when = True,
        setClause = emptySetClause,
        removeClause = emptyRemoveClause)

    val expected =
      VertexConstructRelation(
        Reference("x"),
        relation =
          Project(
            relation =
              GroupBy(
                reference = Reference("x"),
                relation = Select(bindingTable, True),
                groupingAttributes = Seq(groupDeclaration),
                aggregateFunctions = Seq.empty,
                having = None),
            attributes = Set(Reference("x"))),
        expr = emptyObjConstruct,
        setClause = None,
        removeClause = None)

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    assert(actual == expected)
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
        groupDeclaration = None,
        objConstructPattern = objConstructPattern,
        when = True,
        setClause = setClause,
        removeClause = emptyRemoveClause)

    val expected =
      VertexConstructRelation(
        Reference("x"),
        relation =
          Project(
            relation =
              GroupBy(
                reference = Reference("x"),
                relation = Select(bindingTable, True),
                groupingAttributes = Seq.empty,
                aggregateFunctions = Seq.empty,
                having = None),
            attributes = Set(Reference("x"), Reference("c"), Reference("f"))),
        expr = objConstructPattern,
        setClause = Some(setClause),
        removeClause = None)

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    assert(actual == expected)
  }

  test("Create vertex from unbound variable, aggregates are passed to GroupBy and replaced with " +
    "a PropertyRef inline or in SET - " +
    "CONSTRUCT (x {prop1 := AVG(c.prop1)}) SET x.prop2 := MIN(c.prop2)") {
    VarBinder.reset()
    val avg = Avg(distinct = false, PropertyRef(Reference("c"), PropertyKey("prop1")))
    val min = Min(distinct = false, PropertyRef(Reference("c"), PropertyKey("prop2")))
    val algebraTree =
      vertexConstruct(
        reference = Reference("x"),
        groupDeclaration = None,
        objConstructPattern =
          ObjectConstructPattern(
            labelAssignments = LabelAssignments(Seq.empty),
            propAssignments = PropAssignments(Seq(PropAssignment(PropertyKey("prop1"), avg)))
          ),
        when = True,
        setClause =
          SetClause(Seq(PropertySet(Reference("x"), PropAssignment(PropertyKey("prop2"), min)))),
        removeClause = emptyRemoveClause)

    val avgPropAlias = PropertyKey(s"${PROP_AGG_BASENAME}_0")
    val minPropAlias = PropertyKey(s"${PROP_AGG_BASENAME}_1")
    val expected =
      VertexConstructRelation(
        Reference("x"),
        relation =
          Project(
            relation =
              GroupBy(
                reference = Reference("x"),
                relation = Select(bindingTable, True),
                groupingAttributes = Seq.empty,
                aggregateFunctions = Seq(
                  PropertySet(Reference("x"), PropAssignment(avgPropAlias, avg)),
                  PropertySet(Reference("x"), PropAssignment(minPropAlias, min))),
                having = None),
            attributes = Set(Reference("x"))),
        expr =
          ObjectConstructPattern(
            labelAssignments = LabelAssignments(Seq.empty),
            propAssignments =
              PropAssignments(Seq(
                PropAssignment(PropertyKey("prop1"), PropertyRef(Reference("x"), avgPropAlias))
              ))
          ),
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

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    assert(actual == expected)
  }

  test("Create vertex from unbound variable, nested aggregations are passed to the GroupBy - " +
    "CONSTRUCT (x {prop0 := COUNT(*), prop1 := 1 + COUNT(*)}) SET x.prop2 := COUNT(*)") {
    VarBinder.reset()
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
        groupDeclaration = None,
        objConstructPattern =
          ObjectConstructPattern(
            labelAssignments = LabelAssignments(Seq.empty),
            propAssignments =
              PropAssignments(Seq(
                PropAssignment(prop0, count),
                PropAssignment(prop1, onePlusCount)))),
        when = True,
        setClause =
          SetClause(Seq(PropertySet(Reference("x"), PropAssignment(prop2, count)))),
        removeClause = emptyRemoveClause)

    val expected =
      VertexConstructRelation(
        Reference("x"),
        relation =
          Project(
            relation =
              GroupBy(
                reference = Reference("x"),
                relation = Select(bindingTable, True),
                groupingAttributes = Seq.empty,
                aggregateFunctions = Seq(
                  PropertySet(Reference("x"), PropAssignment(aggProp0, count)),
                  PropertySet(Reference("x"), PropAssignment(aggProp1, onePlusCount)),
                  PropertySet(Reference("x"), PropAssignment(aggProp2, count))),
                having = None),
            attributes = Set(Reference("x"))),
        expr =
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

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    assert(actual == expected)
  }

  private def vertexConstruct(reference: Reference,
                              groupDeclaration: Option[GroupDeclaration],
                              objConstructPattern: ObjectConstructPattern,
                              when: AlgebraExpression,
                              setClause: SetClause,
                              removeClause: RemoveClause): ConstructClause = {
    ConstructClause(
      graphs = GraphUnion(Seq.empty),
      CondConstructClause(
        condConstructs = Seq(
          BasicConstructClause(
            constructPattern =
              ConstructPattern(
                topology = Seq(
                  VertexConstruct(
                    reference,
                    copyPattern = None,
                    groupDeclaration,
                    expr = objConstructPattern))),
            when
          ))
      ),
      setClause, removeClause)
  }

  private def extractConstructRelations(constructTree: AlgebraTreeNode): Seq[AlgebraTreeNode] = {
    constructTree.children(1).children
  }
}
