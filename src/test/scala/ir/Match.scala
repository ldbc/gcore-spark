package ir

import org.scalatest.FunSuite
import utils.Gcore

trait Match { this: FunSuite =>

  /** MATCH combines FullGraphPatterns into a list and marks the optionals accordingly. */
  def matchPatternMix(gcore: Gcore, rewriter: SpoofaxToIrRewriter): Unit = {

    def irOfMatch(query: String): Node = {
      rewriter.process(gcore.parseQuery(query).getSubterm(2))
    }

    val foo = VertexMatchPattern(VarDef(Identifier("foo")), ObjectMatchPattern(None(), None()))
    val bar = VertexMatchPattern(VarDef(Identifier("bar")), ObjectMatchPattern(None(), None()))
    val baz = VertexMatchPattern(VarDef(Identifier("baz")), ObjectMatchPattern(None(), None()))
    val noEdge = List.empty

    val matchQuery: String = "" +
      "CONSTRUCT () MATCH (foo)"
    val matchIr: Node =
      MatchClause(
        List(
          FullGraphPatternCondition(
            FullGraphPattern(
              List(
                BasicGraphPatternLocation(BasicGraphPattern(foo, noEdge), /*Location=*/ None()))),
            /*WhereClause = */ None())))
    test(matchQuery) {
      val ast: Node = irOfMatch(matchQuery)
      assert(ast == matchIr)
    }

    val matchLocation: String = "" +
      "CONSTRUCT () MATCH (foo) ON social_graph"
    val matchLocationIr: Node =
      MatchClause(
        List(
          FullGraphPatternCondition(
            FullGraphPattern(
              List(BasicGraphPatternLocation(BasicGraphPattern(foo, noEdge), Location()))),
            /*WhereClause = */ None())))
    test(matchLocation) {
      val ast: Node = irOfMatch(matchLocation)
      assert(ast == matchLocationIr)
    }

    val matchMultiplePatterns: String = "" +
      "CONSTRUCT () MATCH (foo) ON social_graph, (bar)"
    val matchMultiplePatternsIr: Node =
      MatchClause(
        List(
          FullGraphPatternCondition(
            FullGraphPattern(
              List(
                BasicGraphPatternLocation(BasicGraphPattern(foo, noEdge), Location()), // (foo) on social_graph
                BasicGraphPatternLocation(BasicGraphPattern(bar, noEdge), None()))),   // (bar)
            /*WhereClause = */ None())))
    test(matchMultiplePatterns) {
      val ast: Node = irOfMatch(matchMultiplePatterns)
      assert(ast == matchMultiplePatternsIr)
    }

    val matchWhere: String = "" +
      "CONSTRUCT () MATCH (foo) WHERE foo.bar = 1"
    val matchWhereIr: Node =
      MatchClause(
        List(
          FullGraphPatternCondition(
            FullGraphPattern(
              List(
                BasicGraphPatternLocation(BasicGraphPattern(foo, noEdge), None()))),
            WhereClause())))
    test(matchWhere) {
      val ast: Node = irOfMatch(matchWhere)
      assert(ast == matchWhereIr)
    }

    val matchLocationWhere: String = "" +
      "CONSTRUCT () MATCH (foo) ON social_graph WHERE foo.bar = 1"
    val matchLocationWhereIr: Node =
      MatchClause(
        List(
          FullGraphPatternCondition(
            FullGraphPattern(
              List(
                BasicGraphPatternLocation(BasicGraphPattern(foo, noEdge), Location()))),
            WhereClause())))
    test(matchLocationWhere) {
      val ast: Node = irOfMatch(matchLocationWhere)
      assert(ast == matchLocationWhereIr)
    }

    val matchMultiplePatternsWhere: String = "" +
      "CONSTRUCT () MATCH (foo) ON social_graph, (baz) WHERE foo.bar = 1"
    val matchMultiplePatternsWhereIr: Node =
      MatchClause(
        List(
          FullGraphPatternCondition(
            FullGraphPattern(
              List(
                BasicGraphPatternLocation(BasicGraphPattern(foo, noEdge), Location()), // (foo) on social_graph
                BasicGraphPatternLocation(BasicGraphPattern(baz, noEdge), None()))),   // (baz)
            WhereClause())))
    test(matchMultiplePatternsWhere) {
      val ast: Node = irOfMatch(matchMultiplePatternsWhere)
      assert(ast == matchMultiplePatternsWhereIr)
    }

    val matchOptional: String = "" +
      "CONSTRUCT () MATCH (foo) OPTIONAL (bar)"
    val matchOptionalIr: Node =
      MatchClause(
        List(
          FullGraphPatternCondition(
            FullGraphPattern(
              List(BasicGraphPatternLocation(BasicGraphPattern(foo, noEdge), None()))),
            /*WhereClause = */ None()),
          FullGraphPatternCondition(
            FullGraphPattern(
              List(BasicGraphPatternLocation(BasicGraphPattern(bar, noEdge), None()))),
            /*WhereClause = */ None(),
            isOptional = true)))
    test(matchOptional) {
      val ast: Node = irOfMatch(matchOptional)
      assert(ast == matchOptionalIr)
    }

    val matchMultipleOptionals: String = "" +
      "CONSTRUCT () MATCH (foo) OPTIONAL (bar) OPTIONAL (baz)"
    val matchMultipleOptionalsIr: Node =
      MatchClause(
        List(
          FullGraphPatternCondition(
            FullGraphPattern(
              List(BasicGraphPatternLocation(BasicGraphPattern(foo, noEdge), None()))),
            /*WhereClause = */ None()),
          FullGraphPatternCondition(
            FullGraphPattern(
              List(BasicGraphPatternLocation(BasicGraphPattern(bar, noEdge), None()))),
            /*WhereClause = */ None(),
            isOptional = true),
          FullGraphPatternCondition(
            FullGraphPattern(
              List(BasicGraphPatternLocation(BasicGraphPattern(baz, noEdge), None()))),
            /*WhereClause = */ None(),
            isOptional = true)))
    test(matchMultipleOptionals) {
      val ast: Node = irOfMatch(matchMultipleOptionals)
      assert(ast == matchMultipleOptionalsIr)
    }

    val matchMultipleOptionalsWhere: String = "" +
      "CONSTRUCT () MATCH (foo) WHERE foo.prop = 1 " +
      "OPTIONAL (bar) WHERE bar.prop = 2 " +
      "OPTIONAL (baz) WHERE baz.prop = 3 "
    val matchMultipleOptionalsWhereIr: Node =
      MatchClause(
        List(
          FullGraphPatternCondition(
            FullGraphPattern(
              List(BasicGraphPatternLocation(BasicGraphPattern(foo, noEdge), None()))),
            WhereClause()),
          FullGraphPatternCondition(
            FullGraphPattern(
              List(BasicGraphPatternLocation(BasicGraphPattern(bar, noEdge), None()))),
            WhereClause(),
            isOptional = true),
          FullGraphPatternCondition(
            FullGraphPattern(
              List(BasicGraphPatternLocation(BasicGraphPattern(baz, noEdge), None()))),
            WhereClause(),
            isOptional = true)))
    test(matchMultipleOptionalsWhere) {
      val ast: Node = irOfMatch(matchMultipleOptionalsWhere)
      assert(ast == matchMultipleOptionalsWhereIr)
    }
  }
}
