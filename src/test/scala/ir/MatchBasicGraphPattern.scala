package ir

import ir.spoofax._
import org.scalatest.FunSuite
import utils.Gcore

trait MatchBasicGraphPattern { this: FunSuite =>

  /** MATCH assigns a name to unnamed variables. */
  def rewriteUnnamedVariables(gcore: Gcore, rewriter: SpoofaxToIrRewriter): Unit = {
    def irOfBasicGraphPattern(query: String): Node = {
      rewriter.process(
        gcore.parseQuery(query)
          .getSubterm(2) // Match
          .getSubterm(0) // FullGraphPatternCondition
          .getSubterm(0) // FullGraphPattern
          .getSubterm(0) // List
          .getSubterm(0) // BasicGraphPatternLocation
          .getSubterm(0) // BasicGraphPattern
      )
    }

    val vertex: String = "" +
      "CONSTRUCT () MATCH ()"
    val vertexIr: Node =
      BasicGraphPattern(
        VertexMatchPattern(VarDef(Identifier("v_0")), ObjectMatchPattern(None(), None())),
        /*EdgeVertexMatchPattern=*/ List.empty)
    test(vertex) {
      val ast: Node = irOfBasicGraphPattern(vertex)
      assert(ast == vertexIr)
    }

    val outConn: String = "" +
      "CONSTRUCT () MATCH (n)-->(m)"
    val outConnIr: Node =
      BasicGraphPattern(
        VertexMatchPattern(VarDef(Identifier("n")), ObjectMatchPattern(None(), None())),
        List(
          EdgeVertexMatchPattern(
            new OutConn,
            // e_1 instead of e_0 because we are reusing the rewriter object
            EdgeMatchPattern(VarDef(Identifier("e_1")), ObjectMatchPattern(None(), None())),
            VertexMatchPattern(VarDef(Identifier("m")), ObjectMatchPattern(None(), None())))))
    test(outConn) {
      val ast: Node = irOfBasicGraphPattern(outConn)
      assert(ast == outConnIr)
    }

    val outConnPattern: String = "" +
      "CONSTRUCT () MATCH (n)-[]->(m)"
    val outConnPatternIr: Node =
      BasicGraphPattern(
        VertexMatchPattern(VarDef(Identifier("n")), ObjectMatchPattern(None(), None())),
        List(
          EdgeVertexMatchPattern(
            new OutConn,
            EdgeMatchPattern(VarDef(Identifier("e_2")), ObjectMatchPattern(None(), None())),
            VertexMatchPattern(VarDef(Identifier("m")), ObjectMatchPattern(None(), None())))))
    test(outConnPattern) {
      val ast: Node = irOfBasicGraphPattern(outConnPattern)
      assert(ast == outConnPatternIr)
    }

    val outEdge: String = "" +
      "CONSTRUCT () MATCH (n)->(m)"
    val outEdgeIr: Node =
      BasicGraphPattern(
        VertexMatchPattern(VarDef(Identifier("n")), ObjectMatchPattern(None(), None())),
        List(
          EdgeVertexMatchPattern(
            new OutConn,
            EdgeMatchPattern(VarDef(Identifier("e_3")), ObjectMatchPattern(None(), None())),
            VertexMatchPattern(VarDef(Identifier("m")), ObjectMatchPattern(None(), None())))))
    test(outEdge) {
      val ast: Node = irOfBasicGraphPattern(outEdge)
      assert(ast == outEdgeIr)
    }

    val inConn: String = "" +
      "CONSTRUCT () MATCH (n)<--(m)"
    val inConnIr: Node =
      BasicGraphPattern(
        VertexMatchPattern(VarDef(Identifier("n")), ObjectMatchPattern(None(), None())),
        List(
          EdgeVertexMatchPattern(
            new InConn,
            EdgeMatchPattern(VarDef(Identifier("e_4")), ObjectMatchPattern(None(), None())),
            VertexMatchPattern(VarDef(Identifier("m")), ObjectMatchPattern(None(), None())))))
    test(inConn) {
      val ast: Node = irOfBasicGraphPattern(inConn)
      assert(ast == inConnIr)
    }

    val inConnPattern: String = "" +
      "CONSTRUCT () MATCH (n)<-[]-(m)"
    val inConnPatternIr: Node =
      BasicGraphPattern(
        VertexMatchPattern(VarDef(Identifier("n")), ObjectMatchPattern(None(), None())),
        List(
          EdgeVertexMatchPattern(
            new InConn,
            EdgeMatchPattern(VarDef(Identifier("e_5")), ObjectMatchPattern(None(), None())),
            VertexMatchPattern(VarDef(Identifier("m")), ObjectMatchPattern(None(), None())))))
    test(inConnPattern) {
      val ast: Node = irOfBasicGraphPattern(inConnPattern)
      assert(ast == inConnPatternIr)
    }

    val inEdge: String = "" +
      "CONSTRUCT () MATCH (n)<-(m)"
    val inEdgeIr: Node =
      BasicGraphPattern(
        VertexMatchPattern(VarDef(Identifier("n")), ObjectMatchPattern(None(), None())),
        List(
          EdgeVertexMatchPattern(
            new InConn,
            EdgeMatchPattern(VarDef(Identifier("e_6")), ObjectMatchPattern(None(), None())),
            VertexMatchPattern(VarDef(Identifier("m")), ObjectMatchPattern(None(), None())))))
    test(inEdge) {
      val ast: Node = irOfBasicGraphPattern(inEdge)
      assert(ast == inEdgeIr)
    }

    val inOutEdgeNoPattern: String = "" +
      "CONSTRUCT () MATCH (n)<-->(m)"
    val inOutEdgeNoPatternIr: Node =
      BasicGraphPattern(
        VertexMatchPattern(VarDef(Identifier("n")), ObjectMatchPattern(None(), None())),
        List(
          EdgeVertexMatchPattern(
            new InOutConn,
            EdgeMatchPattern(VarDef(Identifier("e_7")), ObjectMatchPattern(None(), None())),
            VertexMatchPattern(VarDef(Identifier("m")), ObjectMatchPattern(None(), None())))))
    test(inOutEdgeNoPattern) {
      val ast: Node = irOfBasicGraphPattern(inOutEdgeNoPattern)
      assert(ast == inOutEdgeNoPatternIr)
    }

    val inOutEdgePattern: String = "" +
      "CONSTRUCT () MATCH (n)<-[]->(m)"
    val inOutEdgePatternIr: Node =
      BasicGraphPattern(
        VertexMatchPattern(VarDef(Identifier("n")), ObjectMatchPattern(None(), None())),
        List(
          EdgeVertexMatchPattern(
            new InOutConn,
            EdgeMatchPattern(VarDef(Identifier("e_8")), ObjectMatchPattern(None(), None())),
            VertexMatchPattern(VarDef(Identifier("m")), ObjectMatchPattern(None(), None())))))
    test(inOutEdgePattern) {
      val ast: Node = irOfBasicGraphPattern(inOutEdgePattern)
      assert(ast == inOutEdgePatternIr)
    }

    val inOutEdge: String = "" +
      "CONSTRUCT () MATCH (n)<->(m)"
    val inOutEdgeIr: Node =
      BasicGraphPattern(
        VertexMatchPattern(VarDef(Identifier("n")), ObjectMatchPattern(None(), None())),
        List(
          EdgeVertexMatchPattern(
            new InOutConn,
            EdgeMatchPattern(VarDef(Identifier("e_9")), ObjectMatchPattern(None(), None())),
            VertexMatchPattern(VarDef(Identifier("m")), ObjectMatchPattern(None(), None())))))
    test(inOutEdge) {
      val ast: Node = irOfBasicGraphPattern(inOutEdge)
      assert(ast == inOutEdgeIr)
    }

    val undirectedEdgeNoPattern: String = "" +
      "CONSTRUCT () MATCH (n)--(m)"
    val undirectedEdgeNoPatternIr: Node =
      BasicGraphPattern(
        VertexMatchPattern(VarDef(Identifier("n")), ObjectMatchPattern(None(), None())),
        List(
          EdgeVertexMatchPattern(
            new UndirectedConn,
            EdgeMatchPattern(VarDef(Identifier("e_10")), ObjectMatchPattern(None(), None())),
            VertexMatchPattern(VarDef(Identifier("m")), ObjectMatchPattern(None(), None())))))
    test(undirectedEdgeNoPattern) {
      val ast: Node = irOfBasicGraphPattern(undirectedEdgeNoPattern)
      assert(ast == undirectedEdgeNoPatternIr)
    }

    val undirectedEdgePattern: String = "" +
      "CONSTRUCT () MATCH (n)-[]-(m)"
    val undirectedEdgePatternIr: Node =
      BasicGraphPattern(
        VertexMatchPattern(VarDef(Identifier("n")), ObjectMatchPattern(None(), None())),
        List(
          EdgeVertexMatchPattern(
            new UndirectedConn,
            EdgeMatchPattern(VarDef(Identifier("e_11")), ObjectMatchPattern(None(), None())),
            VertexMatchPattern(VarDef(Identifier("m")), ObjectMatchPattern(None(), None())))))
    test(undirectedEdgePattern) {
      val ast: Node = irOfBasicGraphPattern(undirectedEdgePattern)
      assert(ast == undirectedEdgePatternIr)
    }

    val undirectedEdge: String = "" +
      "CONSTRUCT () MATCH (n)-(m)"
    val undirectedEdgeIr: Node =
      BasicGraphPattern(
        VertexMatchPattern(VarDef(Identifier("n")), ObjectMatchPattern(None(), None())),
        List(
          EdgeVertexMatchPattern(
            new UndirectedConn,
            EdgeMatchPattern(VarDef(Identifier("e_12")), ObjectMatchPattern(None(), None())),
            VertexMatchPattern(VarDef(Identifier("m")), ObjectMatchPattern(None(), None())))))
    test(undirectedEdge) {
      val ast: Node = irOfBasicGraphPattern(undirectedEdge)
      assert(ast == undirectedEdgeIr)
    }
  }

  /** ObjectMatchPatterns are built with Labels and Property's. */
  def objectMatchPattern(gcore: Gcore, rewriter: SpoofaxToIrRewriter): Unit = {
    def irOfVertexObjectMatchPattern(query: String): Node = {
      rewriter.process(
        gcore.parseQuery(query)
          .getSubterm(2) // Match
          .getSubterm(0) // FullGraphPatternCondition
          .getSubterm(0) // FullGraphPattern
          .getSubterm(0) // List
          .getSubterm(0) // BasicGraphPatternLocation
          .getSubterm(0) // BasicGraphPattern
          .getSubterm(0) // Vertex
          .getSubterm(1) // ObjectMatchPattern
      )
    }

    def irOfEdgeObjectMatchPattern(query: String): Node = {
      rewriter.process(
        gcore.parseQuery(query)
          .getSubterm(2) // Match
          .getSubterm(0) // FullGraphPatternCondition
          .getSubterm(0) // FullGraphPattern
          .getSubterm(0) // List
          .getSubterm(0) // BasicGraphPatternLocation
          .getSubterm(0) // BasicGraphPattern
          .getSubterm(1) // List
          .getSubterm(0) // EdgeVertexMatchPattern
          .getSubterm(0) // UndirectedEdge
          .getSubterm(0) // Some
          .getSubterm(0) // Edge
          .getSubterm(0) // EdgeMatchPattern
          .getSubterm(1) // ObjectMatchPattern
      )
    }

    val nodeLabel: String = "" +
      "CONSTRUCT () MATCH (foo:bar)"
    val nodeLabelIr: Node =
      ObjectMatchPattern(
        LabelPredicates(List(DisjunctLabels(List(Label(Identifier("bar")))))),
        /*PropertyPredicates=*/ None())
    test(nodeLabel) {
      val ast: Node = irOfVertexObjectMatchPattern(nodeLabel)
      assert(ast == nodeLabelIr)
    }

    val nodeDisjLabels: String = "" +
      "CONSTRUCT () MATCH (foo:bar|baz)"
    val nodeDisjLabelsIr: Node =
      ObjectMatchPattern(
        LabelPredicates(List(
          DisjunctLabels(List(
            Label(Identifier("bar")),
            Label(Identifier("baz")))))),
        /*PropertyPredicates=*/ None())
    test(nodeDisjLabels) {
      val ast: Node = irOfVertexObjectMatchPattern(nodeDisjLabels)
      assert(ast == nodeDisjLabelsIr)
    }

    val nodeConjLabels: String = "" +
      "CONSTRUCT () MATCH (foo:bar:baz|qux)"
    val nodeConjLabelsIr: Node =
      ObjectMatchPattern(
        LabelPredicates(
          List(
            DisjunctLabels(List(Label(Identifier("bar")))),
            DisjunctLabels(List(Label(Identifier("baz")), Label(Identifier("qux")))))),
        /*PropertyPredicates=*/ None())
    test(nodeConjLabels) {
      val ast: Node = irOfVertexObjectMatchPattern(nodeConjLabels)
      assert(ast == nodeConjLabelsIr)
    }

    val nodeProp: String = "" +
      "CONSTRUCT () MATCH (foo {bar = baz, qux = fred})"
    val nodePropIr: Node =
      ObjectMatchPattern(
        /*LabelPredicates=*/ None(),
        PropertyPredicates(List(
          Property(Identifier("bar"), new Expression),
          Property(Identifier("qux"), new Expression))))
    test(nodeProp) {
      val ast: Node = irOfVertexObjectMatchPattern(nodeProp)
      assert(ast == nodePropIr)
    }

    val nodeLabelProp: String = "" +
      "CONSTRUCT () MATCH (foo:bar {baz = qux})"
    val nodeLabelPropIr: Node =
      ObjectMatchPattern(
        LabelPredicates(List(DisjunctLabels(List(Label(Identifier("bar")))))),
        PropertyPredicates(List(Property(Identifier("baz"), new Expression))))
    test(nodeLabelProp) {
      val ast: Node = irOfVertexObjectMatchPattern(nodeLabelProp)
      assert(ast == nodeLabelPropIr)
    }

    val edgeLabel: String = "" +
      "CONSTRUCT () MATCH (foo)-[bar:baz]-(qux)"
    val edgeLabelIr: Node =
      ObjectMatchPattern(
        LabelPredicates(List(DisjunctLabels(List(Label(Identifier("baz")))))),
        /*PropertyPredicates=*/ None())
    test(edgeLabel) {
      val ast: Node = irOfEdgeObjectMatchPattern(edgeLabel)
      assert(ast == edgeLabelIr)
    }

    val edgeDisjLabels: String = "" +
      "CONSTRUCT () MATCH (foo)-[bar:baz|fred]-(qux)"
    val edgeDisjLabelsIr: Node =
      ObjectMatchPattern(
        LabelPredicates(List(
          DisjunctLabels(List(
            Label(Identifier("baz")),
            Label(Identifier("fred")))))),
        /*PropertyPredicates=*/ None())
    test(edgeDisjLabels) {
      val ast: Node = irOfEdgeObjectMatchPattern(edgeDisjLabels)
      assert(ast == edgeDisjLabelsIr)
    }

    val edgeConjLabels: String = "" +
      "CONSTRUCT () MATCH (foo)-[bar:baz:fred|waldo]-(qux)"
    val edgeConjLabelsIr: Node =
      ObjectMatchPattern(
        LabelPredicates(
          List(
            DisjunctLabels(List(Label(Identifier("baz")))),
            DisjunctLabels(List(Label(Identifier("fred")), Label(Identifier("waldo")))))),
        /*PropertyPredicates=*/ None())
    test(edgeConjLabels) {
      val ast: Node = irOfEdgeObjectMatchPattern(edgeConjLabels)
      assert(ast == edgeConjLabelsIr)
    }

    val edgeProp: String = "" +
      "CONSTRUCT () MATCH (foo)-[bar {baz = fred, waldo = corge}]-(qux)"
    val edgePropIr: Node =
      ObjectMatchPattern(
        /*LabelPredicates=*/ None(),
        PropertyPredicates(List(
          Property(Identifier("baz"), new Expression),
          Property(Identifier("waldo"), new Expression))))
    test(edgeProp) {
      val ast: Node = irOfEdgeObjectMatchPattern(edgeProp)
      assert(ast == edgePropIr)
    }

    val edgeLabelProp: String = "" +
      "CONSTRUCT () MATCH (foo)-[bar:baz {fred = waldo}]-(qux)"
    val edgeLabelPropIr: Node =
      ObjectMatchPattern(
        LabelPredicates(List(DisjunctLabels(List(Label(Identifier("baz")))))),
        PropertyPredicates(List(Property(Identifier("fred"), new Expression))))
    test(edgeLabelProp) {
      val ast: Node = irOfEdgeObjectMatchPattern(edgeLabelProp)
      assert(ast == edgeLabelPropIr)
    }
  }

  /** Multiple EdgeVertexMatchPatterns are assigned to their respective BasicGraphPattern. */
  def basicGraphPattern(gcore: Gcore, rewriter: SpoofaxToIrRewriter): Unit = {
    def irOfBasicGraphPattern(query: String): Node = {
      rewriter.process(
        gcore.parseQuery(query)
          .getSubterm(2) // Match
          .getSubterm(0) // FullGraphPatternCondition
          .getSubterm(0) // FullGraphPattern
          .getSubterm(0) // List
          .getSubterm(0) // BasicGraphPatternLocation
          .getSubterm(0) // BasicGraphPattern
      )
    }

    val pattern: String = "" +
      "CONSTRUCT () MATCH (foo)-[qux]->(bar)-[fred]->(baz)"
    val patternIr: Node =
      BasicGraphPattern(
        VertexMatchPattern(
          VarDef(Identifier("foo")),
          ObjectMatchPattern(None(), None())),
        List(
          EdgeVertexMatchPattern(
            new OutConn,
            EdgeMatchPattern(
              VarDef(Identifier("qux")),
              ObjectMatchPattern(None(), None())),
            VertexMatchPattern(
              VarDef(Identifier("bar")),
              ObjectMatchPattern(None(), None()))),
          EdgeVertexMatchPattern(
            new OutConn,
            EdgeMatchPattern(
              VarDef(Identifier("fred")),
              ObjectMatchPattern(None(), None())),
            VertexMatchPattern(
              VarDef(Identifier("baz")),
              ObjectMatchPattern(None(), None())))
        )
      )
    test(pattern) {
      val ast: Node = irOfBasicGraphPattern(pattern)
      assert(ast == patternIr)
    }
  }
}
