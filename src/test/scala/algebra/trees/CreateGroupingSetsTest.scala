package algebra.trees

import algebra.expressions._
import algebra.operators._
import algebra.trees.CreateGroupingSets.PROP_AGG_BASENAME
import algebra.types._
import org.scalatest.{FunSuite, Matchers}
import parser.utils.VarBinder
import schema.GraphDb

class CreateGroupingSetsTest extends FunSuite with Matchers {

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

  test("Create vertex from bound variable - CONSTRUCT (c)") {
    val algebraTree =
      vertexConstruct(
        reference = Reference("c"),
        groupDeclaration = None,
        objConstructPattern = ObjectConstructPattern.empty,
        when = True,
        setClause = emptySetClause,
        removeClause = emptyRemoveClause)

    val expected = matchedVertex(reference = Reference("c"))
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
      matchedVertex(
        reference = Reference("c"),
        objectConstructPattern = objectConstructPattern)

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
        objConstructPattern = ObjectConstructPattern.empty,
        when = True,
        setClause = SetClause(Seq(prop1, prop2, prop3)),
        removeClause = emptyRemoveClause)

    val expected =
      matchedVertex(
        reference = Reference("c"),
        setClause = Some(SetClause(Seq(prop1, prop2)))
      )

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
        objConstructPattern = ObjectConstructPattern.empty,
        when = True,
        setClause = emptySetClause,
        removeClause =
          RemoveClause(
            propRemoves = Seq(prop1, prop2),
            labelRemoves = Seq(label1, label2))
      )

    val expected =
      matchedVertex(
        reference = Reference("c"),
        removeClause = Some(RemoveClause(propRemoves = Seq(prop1), labelRemoves = Seq(label1)))
      )

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
        objConstructPattern = ObjectConstructPattern.empty,
        when = condition,
        setClause = emptySetClause,
        removeClause = emptyRemoveClause)

    val expected =
      matchedVertex(
        reference = Reference("c"),
        when = condition)

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    assert(actual == expected)
  }

  test("Create vertex from unbound variable - CONSTRUCT (x)") {
    val algebraTree =
      vertexConstruct(
        reference = Reference("x"),
        groupDeclaration = None,
        objConstructPattern = ObjectConstructPattern.empty,
        when = True,
        setClause = emptySetClause,
        removeClause = emptyRemoveClause)

    val expected = unmatchedVertexNoGrouping(reference = Reference("x"))

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    assert(actual == expected)
  }

  test("Create vertex from unbound variable, GROUPing attributes are passed to GroupBy - " +
    "CONSTRUCT (x GROUP c.prop1)") {
    val groupingProps = Seq(PropertyRef(Reference("c"), PropertyKey("prop1")))
    val algebraTree =
      vertexConstruct(
        reference = Reference("x"),
        groupDeclaration = Some(GroupDeclaration(groupingProps)),
        objConstructPattern = ObjectConstructPattern.empty,
        when = True,
        setClause = emptySetClause,
        removeClause = emptyRemoveClause)

    val expected =
      unmatchedVertexGrouping(
        reference = Reference("x"),
        groupingProps = groupingProps)

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
      unmatchedVertexNoGrouping(
        reference = Reference("x"),
        objectConstructPattern = objConstructPattern,
        setClause = Some(setClause))

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    assert(actual == expected)
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
        when = True,
        setClause =
          SetClause(Seq(PropertySet(Reference("x"), PropAssignment(PropertyKey("prop2"), min)))),
        removeClause = emptyRemoveClause)

    val avgPropAlias = PropertyKey(s"${PROP_AGG_BASENAME}_0")
    val minPropAlias = PropertyKey(s"${PROP_AGG_BASENAME}_1")
    val expected =
      unmatchedVertexGrouping(
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

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    assert(actual == expected)
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
        when = True,
        setClause =
          SetClause(Seq(PropertySet(Reference("x"), PropAssignment(prop2, count)))),
        removeClause = emptyRemoveClause)

    val expected =
      unmatchedVertexGrouping(
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

    val actual = extractConstructRelations(rewriter rewriteTree algebraTree).head
    assert(actual == expected)
  }

  test("Edge is created from inner-joining the binding table with endpoints' constructs. Edge " +
    "and endpoints are unbound, to simplify their relations.") {
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

    actual should matchPattern {
      case
        EdgeConstructRelation(
          Reference("e_0"),
          Project(
            EntityConstructRelation(
              Reference("e_0"),
              /*isMatchedRef =*/ _,
              AddColumn(
                Reference("e_0"),
                InnerJoin(
                  InnerJoin(
                    /*lhs =*/ Select(`bindingTable`, _, _),
                    /*rhs =*/ EntityConstructRelation(Reference("c_0"), _, _, _, _, _, _),
                    /*bset =*/ _),
                  /*rhs =*/ EntityConstructRelation(Reference("f_0"), _, _, _, _, _, _),
                  /*bset =*/ _)),
              /*groupedAttributes =*/ _,
              /*expr =*/ _,
              /*setClause =*/ _,
              /*removeClause =*/ _),
            /*attributes =*/ _),
          /*leftReference =*/ Reference("c_0"),
          /*rightReference =*/ Reference("f_0"),
          /*connType =*/ OutConn) =>
    }
  }

  private def vertexConstruct(reference: Reference,
                              groupDeclaration: Option[GroupDeclaration],
                              objConstructPattern: ObjectConstructPattern,
                              when: AlgebraExpression,
                              setClause: SetClause,
                              removeClause: RemoveClause): ConstructClause = {
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
                            groupDeclaration: Option[GroupDeclaration],
                            objConstructPattern: ObjectConstructPattern,
                            when: AlgebraExpression,
                            setClause: SetClause,
                            removeClause: RemoveClause): ConstructClause = {
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
                                          when: AlgebraExpression,
                                          setClause: SetClause,
                                          removeClause: RemoveClause): ConstructClause = {
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

  private def matchedVertex(reference: Reference,
                            objectConstructPattern: ObjectConstructPattern =
                            ObjectConstructPattern.empty,
                            setClause: Option[SetClause] = None,
                            removeClause: Option[RemoveClause] = None,
                            when: AlgebraExpression = True): VertexConstructRelation = {
    VertexConstructRelation(
      reference,
      Project(
        EntityConstructRelation(
          reference,
          isMatchedRef = true,
          GroupBy(
            reference,
            relation = Select(bindingTable, when),
            groupingAttributes = Seq(reference),
            aggregateFunctions = Seq.empty,
            having = None),
          groupedAttributes = Seq.empty,
          expr = objectConstructPattern,
          setClause,
          removeClause),
        attributes = Set(reference)))
  }

  private def unmatchedVertexNoGrouping(reference: Reference,
                                        objectConstructPattern: ObjectConstructPattern =
                                        ObjectConstructPattern.empty,
                                        setClause: Option[SetClause] = None,
                                        removeClause: Option[RemoveClause] = None,
                                        when: AlgebraExpression = True): VertexConstructRelation = {
    VertexConstructRelation(
      reference,
      Project(
        EntityConstructRelation(
          reference,
          isMatchedRef = false,
          AddColumn(
            reference,
            Select(bindingTable, when)),
          groupedAttributes = Seq.empty,
          expr = objectConstructPattern,
          setClause,
          removeClause),
        attributes = Set(reference)))
  }

  private def unmatchedVertexGrouping(reference: Reference,
                                      groupingProps: Seq[PropertyRef],
                                      aggregateFunctions: Seq[PropertySet] = Seq.empty,
                                      objectConstructPattern: ObjectConstructPattern =
                                      ObjectConstructPattern.empty,
                                      setClause: Option[SetClause] = None,
                                      removeClause: Option[RemoveClause] = None,
                                      when: AlgebraExpression = True): VertexConstructRelation = {
    VertexConstructRelation(
      reference,
      Project(
        EntityConstructRelation(
          reference,
          isMatchedRef = false,
          AddColumn(
            reference,
            GroupBy(
              reference,
              relation = Select(bindingTable, when),
              groupingAttributes = Seq(GroupDeclaration(groupingProps)),
              aggregateFunctions,
              having = None)),
          groupedAttributes = groupingProps,
          expr = objectConstructPattern,
          setClause,
          removeClause),
        attributes = Set(reference)))
  }

  private def extractConstructRelations(constructTree: AlgebraTreeNode): Seq[AlgebraTreeNode] = {
    val condConstructClause = constructTree.children(1)
    val constructRelations = condConstructClause.children
    constructRelations
  }
}
