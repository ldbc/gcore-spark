package ir.trees.algebra

import ir.algebra.expressions.{Exists, Reference}
import ir.algebra.types._
import ir.algebra.{CondMatchClause, MatchClause, Query, SimpleMatchClause}
import ir.trees.{AlgebraTreeBuilder, SpoofaxTreeBuilder}
import org.scalatest.{FunSuite, Inside, Matchers}
import utils.Gcore

trait SpoofaxToAlgebraMatch extends Matchers with Inside {
  this: FunSuite =>

  val gcore: Gcore = new Gcore()

  def build(): Unit = {

    test("Query has children: MatchClause") {
      val ast = gcore parseQuery
        "CONSTRUCT () MATCH (v)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      algebraTree should matchPattern { case Query(MatchClause(_)) => }
    }
  }

  def matchMixing(): Unit = {

    test("Match with FullGraphPatternCondition and Optional clause") {
      val ast = gcore parseQuery
        "CONSTRUCT () " +
          "MATCH (u) " +
          "OPTIONAL (v)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      algebraTree should matchPattern {
        case Query(MatchClause(List(
        /*u =>*/ CondMatchClause(List(SimpleMatchClause(_, _, /*isOptional =*/ false)), _),
        /*v =>*/ CondMatchClause(List(SimpleMatchClause(_, _, /*isOptional =*/ true)), _)))) =>
      }
    }
  }

  def graphPatterns(): Unit = {

    test("(u) => SimpleMatchClause([Vertex(u)])") {
      val ast = gcore parseQuery
        "CONSTRUCT () " +
          "MATCH (u)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(MatchClause(
          List(CondMatchClause(
            List(SimpleMatchClause(graphPattern, _, _)), _)))) => {

          graphPattern should matchPattern {
            case GraphPattern(
            /*topology =*/ List(Vertex(Reference("u"), _))) =>
          }
        }
      }
    }

    test("(u), (v) => SimpleMatchClause([Vertex(u)]), SimpleMatchClause([Vertex(v)])") {
      val ast = gcore parseQuery
        "CONSTRUCT () " +
          "MATCH (u), (v)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(MatchClause(
          List(CondMatchClause(
            List(
              /*u =>*/ SimpleMatchClause(graphPatternU, _, _),
              /*v =>*/ SimpleMatchClause(graphPatternV, _, _)),
            /*where =*/ _)))) => {

          graphPatternU should matchPattern {
            case GraphPattern(
            /*topology =*/ List(Vertex(Reference("u"), _))) =>
          }

          graphPatternV should matchPattern {
            case GraphPattern(
            /*topology =*/ List(Vertex(Reference("v"), _))) =>
          }
        }
      }
    }

    test("(u)->(v) => SimpleMatchClause([Edge(u->v)])") {
      val ast = gcore parseQuery
        "CONSTRUCT () " +
          "MATCH (u)-[e]->(v)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(MatchClause(
          List(CondMatchClause(
            List(
              /*u->v =>*/ SimpleMatchClause(graphPattern, _, _)),
            /*where =*/ _)))) => {

          graphPattern should matchPattern {
            case GraphPattern(
            /*topology =*/ List(Edge(
                /*connName =*/ Reference("e"),
                /*leftEndpoint =*/ Vertex(Reference("u"), _),
                /*rightEndpoint =*/ Vertex(Reference("v"), _),
                /*connType =*/ OutConn(),
                /*expr =*/ _))) =>
          }
        }
      }
    }

    test("(u)->(v), (v)->(w) => SimpleMatchClause([Edge(u->v)]), SimpleMatchClause([Edge(v->w)])") {
      val ast = gcore parseQuery
        "CONSTRUCT () " +
          "MATCH (u)-[e1]->(v), (v)-[e2]->(w)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(MatchClause(
          List(CondMatchClause(
            List(
              /*u->v =>*/ SimpleMatchClause(graphPattern1, _, _),
              /*v->w =>*/ SimpleMatchClause(graphPattern2, _, _)),
            /*where =*/ _)))) => {

          graphPattern1 should matchPattern {
            case GraphPattern(
            /*topology =*/ List(Edge(
              /*connName =*/ Reference("e1"),
              /*leftEndpoint =*/ Vertex(Reference("u"), _),
              /*rightEndpoint =*/ Vertex(Reference("v"), _),
              /*connType =*/ OutConn(),
              /*expr =*/ _))) =>
          }

          graphPattern2 should matchPattern {
            case GraphPattern(
            /*topology =*/ List(Edge(
                /*connName =*/ Reference("e2"),
                /*leftEndpoint =*/ Vertex(Reference("v"), _),
                /*rightEndpoint =*/ Vertex(Reference("w"), _),
                /*connType =*/ OutConn(),
                /*expr =*/ _))) =>
          }
        }
      }
    }

    test("(u)->(v)->(w) => SimpleMatchClause([Edge(u->v), Edge(v->w)])") {
      val ast = gcore parseQuery
        "CONSTRUCT () " +
          "MATCH (u)-[e1]->(v)-[e2]->(w)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(MatchClause(
          List(CondMatchClause(
            List(
              /*u->v->w =>*/ SimpleMatchClause(graphPattern, _, _)),
            /*where =*/ _)))) => {

          graphPattern should matchPattern { case GraphPattern(List(
          Edge(Reference("e1"), Vertex(Reference("u"), _), Vertex(Reference("v"), _), OutConn(), _),
          Edge(Reference("e2"), Vertex(Reference("v"), _), Vertex(Reference("w"), _), OutConn(), _))
          ) => }
        }
      }
    }

    test("isObj = false for virtual path") {
      val ast = gcore parseQuery
        "CONSTRUCT () " +
          "MATCH (u)-/p/->(v)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(MatchClause(
          List(CondMatchClause(List(/*u-/p/->v =>*/ SimpleMatchClause(graphPattern, _, _)),
          /*where =*/ _)))) => {

          graphPattern should matchPattern {
            case GraphPattern(List(Path(_, _, _, _, _, _, _, false))) => }
        }
      }
    }

    test("isObj = true for objectified path") {
      val ast = gcore parseQuery
        "CONSTRUCT () " +
          "MATCH (u)-/@p/->(v)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(MatchClause(
          List(CondMatchClause(List(/*u-/@p/->v =>*/ SimpleMatchClause(graphPattern, _, _)),
          /*where =*/ _)))) => {

          graphPattern should matchPattern {
            case GraphPattern(List(Path(_, _, _, _, _, _, _, true))) => }
        }
      }
    }
  }

  def expressions(): Unit = {

    test("WHERE BasicGraphPattern => WHERE EXISTS (CONSTRUCT () MATCH BasicGraphPattern") {
      val ast = gcore parseQuery
        "CONSTRUCT () " +
          "MATCH (u) " +
          "WHERE (u:Label)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(MatchClause(
          List(CondMatchClause(/*simpleMatchClauses =*/ _, /*where =*/ expr)))) => {

          expr should matchPattern { case Exists(Query(_)) => }
        }
      }
    }
  }

  def locations(): Unit = {

    test("DefaultGraph is used in SimpleMatchClause when no graph is specified.") {
      val ast = gcore parseQuery
        "CONSTRUCT () " +
          "MATCH (u)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(MatchClause(List(CondMatchClause(List(SimpleMatchClause(_, graph, _)), _)))) => {

          graph should matchPattern { case DefaultGraph() => }
        }
      }
    }

    test("NamedGraph is used in SimpleMatchClause when graph name is specified.") {
      val ast = gcore parseQuery
        "CONSTRUCT () " +
          "MATCH (u) ON social_graph"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(MatchClause(List(CondMatchClause(List(SimpleMatchClause(_, graph, _)), _)))) => {

          graph should matchPattern { case NamedGraph("social_graph") => }
        }
      }
    }

    test("QueryGraph is used in SimpleMatchClause when graph is provided through query.") {
      val ast = gcore parseQuery
        "CONSTRUCT () " +
          "MATCH (u) ON (CONSTRUCT() MATCH (v))"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(MatchClause(List(CondMatchClause(List(SimpleMatchClause(_, graph, _)), _)))) => {

          graph should matchPattern { case QueryGraph(_) => }
        }
      }
    }
  }
}
