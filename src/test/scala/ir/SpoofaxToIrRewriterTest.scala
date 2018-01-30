package ir

import org.scalatest.FunSuite
import utils.Gcore

class SpoofaxToIrRewriterTest extends FunSuite {
  val gcore: Gcore = new Gcore
  val rewriter: SpoofaxToIrRewriter = new SpoofaxToIrRewriter
  val matchQuery: String = "" +
    "CONSTRUCT () MATCH ()"
  val matchLocation: String = "" +
    "CONSTRUCT () MATCH () ON social_graph"
  val matchMultiplePatterns: String = "" +
    "CONSTRUCT () MATCH (foo) ON social_graph, (bar)"
  val matchWhere: String = "" +
    "CONSTRUCT () MATCH (foo) WHERE foo.bar = 1"
  val matchLocationWhere: String = "" +
    "CONSTRUCT () MATCH (foo) ON social_graph WHERE foo.bar = 1"
  val matchMultiplePatternsWhere: String = "" +
    "CONSTRUCT () MATCH (foo) ON social_graph, (baz) WHERE foo.bar = 1"
  val matchOptional: String = "" +
    "CONSTRUCT () MATCH (foo) OPTIONAL (bar)"
  val matchMultipleOptionals: String = "" +
    "CONSTRUCT () MATCH (foo) OPTIONAL (bar) OPTIONAL (baz)"
  val matchMultipleOptionalsWhere: String = "" +
    "CONSTRUCT () MATCH (foo) WHERE foo.prop = 1" +
    "OPTIONAL (bar) WHERE bar.prop = 2" +
    "OPTIONAL (baz) WHERE baz.prop = 3"

  val matchIr: Node =
      MatchClause(
        List(
          FullGraphPatternCondition(
            FullGraphPattern(List(BasicGraphPatternLocation(BasicGraphPattern(), None()))),
            /*WhereClause = */ None())))
  val matchLocationIr: Node =
    MatchClause(
      List(
        FullGraphPatternCondition(
          FullGraphPattern(List(BasicGraphPatternLocation(BasicGraphPattern(), Location()))),
          /*WhereClause = */ None())))
  val matchMultiplePatternsIr: Node =
    MatchClause(
      List(
        FullGraphPatternCondition(
          FullGraphPattern(
            List(
              BasicGraphPatternLocation(BasicGraphPattern(), Location()), // (foo) on social_graph
              BasicGraphPatternLocation(BasicGraphPattern(), None()))),   // (bar)
          /*WhereClause = */ None())))
  val matchWhereIr: Node =
    MatchClause(
      List(
        FullGraphPatternCondition(
          FullGraphPattern(List(BasicGraphPatternLocation(BasicGraphPattern(), None()))),
          WhereClause())))
  val matchLocationWhereIr: Node =
    MatchClause(
      List(
        FullGraphPatternCondition(
          FullGraphPattern(List(BasicGraphPatternLocation(BasicGraphPattern(), Location()))),
          WhereClause())))
  val matchMultiplePatternsWhereIr: Node =
    MatchClause(
      List(
        FullGraphPatternCondition(
          FullGraphPattern(
            List(
              BasicGraphPatternLocation(BasicGraphPattern(), Location()), // (foo) on social_graph
              BasicGraphPatternLocation(BasicGraphPattern(), None()))),   // (baz)
          WhereClause())))
  val matchOptionalIr: Node =
    MatchClause(
      List(
        FullGraphPatternCondition(
          FullGraphPattern(List(BasicGraphPatternLocation(BasicGraphPattern(), None()))),
          /*WhereClause = */ None()),
        FullGraphPatternCondition(
          FullGraphPattern(List(BasicGraphPatternLocation(BasicGraphPattern(), None()))),
          /*WhereClause = */ None(),
          isOptional = true)))
  val matchMultipleOptionalsIr: Node =
    MatchClause(
      List(
        FullGraphPatternCondition(
          FullGraphPattern(List(BasicGraphPatternLocation(BasicGraphPattern(), None()))),
          /*WhereClause = */ None()),
        FullGraphPatternCondition(
          FullGraphPattern(List(BasicGraphPatternLocation(BasicGraphPattern(), None()))),
          /*WhereClause = */ None(),
          isOptional = true),
        FullGraphPatternCondition(
          FullGraphPattern(List(BasicGraphPatternLocation(BasicGraphPattern(), None()))),
          /*WhereClause = */ None(),
          isOptional = true)))
  val matchMultipleOptionalsWhereIr: Node =
    MatchClause(
      List(
        FullGraphPatternCondition(
          FullGraphPattern(List(BasicGraphPatternLocation(BasicGraphPattern(), None()))),
          WhereClause()),
        FullGraphPatternCondition(
          FullGraphPattern(List(BasicGraphPatternLocation(BasicGraphPattern(), None()))),
          WhereClause(),
          isOptional = true),
        FullGraphPatternCondition(
          FullGraphPattern(List(BasicGraphPatternLocation(BasicGraphPattern(), None()))),
          WhereClause(),
          isOptional = true)))

  def irOfMatch(query: String): Node = {
    rewriter.process(gcore.parseQuery(query).getSubterm(2))
  }

  test(matchQuery) {
    val ast: Node = irOfMatch(matchQuery)
    assert(ast == matchIr)
  }

  test(matchLocation) {
    val ast: Node = irOfMatch(matchLocation)
    assert(ast == matchLocationIr)
  }

  test(matchMultiplePatterns) {
    val ast: Node = irOfMatch(matchMultiplePatterns)
    assert(ast == matchMultiplePatternsIr)
  }

  test(matchWhere) {
    val ast: Node = irOfMatch(matchWhere)
    assert(ast == matchWhereIr)
  }

  test(matchLocationWhere) {
    val ast: Node = irOfMatch(matchLocationWhere)
    assert(ast == matchLocationWhereIr)
  }

  test(matchMultiplePatternsWhere) {
    val ast: Node = irOfMatch(matchMultiplePatternsWhere)
    assert(ast == matchMultiplePatternsWhereIr)
  }

  test(matchOptional) {
    val ast: Node = irOfMatch(matchOptional)
    assert(ast == matchOptionalIr)
  }

  test(matchMultipleOptionals) {
    val ast: Node = irOfMatch(matchMultipleOptionals)
    assert(ast == matchMultipleOptionalsIr)
  }

  test(matchMultipleOptionalsWhere) {
    val ast: Node = irOfMatch(matchMultipleOptionalsWhere)
    assert(ast == matchMultipleOptionalsWhereIr)
  }
}
