package parser.trees

import algebra.expressions._
import algebra.operators._
import algebra.types._
import org.scalatest.{FunSuite, Inside, Matchers}
import parser.utils.GcoreLang

trait SpoofaxToAlgebraMatch extends Matchers with Inside {
  this: FunSuite =>

  def build(): Unit = {

    test("Query has children: MatchClause") {
      val ast = GcoreLang parseQuery
        "CONSTRUCT () MATCH (v)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      algebraTree should matchPattern { case Query(MatchClause(_, _)) => }
    }
  }

  def matchMixing(): Unit = {

    test("Match with FullGraphPatternCondition and Optional clause") {
      val ast = GcoreLang parseQuery
        "CONSTRUCT () " +
          "MATCH (u) " +
          "OPTIONAL (v)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      algebraTree should matchPattern {
        case Query(MatchClause(
        /*u =>*/ CondMatchClause(List(SimpleMatchClause(_, _)), _),
        /*v =>*/ List(CondMatchClause(List(SimpleMatchClause(_, _)), _))
        )) =>
      }
    }
  }

  def graphPatterns(): Unit = {

    test("(u) => SimpleMatchClause([Vertex(u)])") {
      val ast = GcoreLang parseQuery
        "CONSTRUCT () " +
          "MATCH (u)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(MatchClause(
          /* non optional */ CondMatchClause(List(SimpleMatchClause(graphPattern, _)), _),
          /* optional */ _
          )) => {

          graphPattern should matchPattern {
            case GraphPattern(
            /*topology =*/ List(Vertex(Reference("u"), _))) =>
          }
        }
      }
    }

    test("(u), (v) => SimpleMatchClause([Vertex(u)]), SimpleMatchClause([Vertex(v)])") {
      val ast = GcoreLang parseQuery
        "CONSTRUCT () " +
          "MATCH (u), (v)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(
          MatchClause(
            /* non optional */ CondMatchClause(
              List(
                /*u =>*/ SimpleMatchClause(graphPatternU, _),
                /*v =>*/ SimpleMatchClause(graphPatternV, _)),
              /*where =*/ _),
            /* optional */ _)) => {

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
      val ast = GcoreLang parseQuery
        "CONSTRUCT () " +
          "MATCH (u)-[e]->(v)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(
          MatchClause(
            /* non optional */ CondMatchClause(
              List(
                /*u->v =>*/ SimpleMatchClause(graphPattern, _)),
              /*where =*/ _),
            /* optional */ _)) => {

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
      val ast = GcoreLang parseQuery
        "CONSTRUCT () " +
          "MATCH (u)-[e1]->(v), (v)-[e2]->(w)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(
          MatchClause(
            /* non optional */ CondMatchClause(
              List(
                /*u->v =>*/ SimpleMatchClause(graphPattern1, _),
                /*v->w =>*/ SimpleMatchClause(graphPattern2, _)),
              /*where =*/ _),
            /* optional */ _
        )) => {

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
      val ast = GcoreLang parseQuery
        "CONSTRUCT () " +
          "MATCH (u)-[e1]->(v)-[e2]->(w)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(
          MatchClause(
            /* non optional */ CondMatchClause(
              List(
                /*u->v->w =>*/ SimpleMatchClause(graphPattern, _)),
              /*where =*/ _),
            /* optional */ _)) => {

          graphPattern should matchPattern { case GraphPattern(List(
          Edge(Reference("e1"), Vertex(Reference("u"), _), Vertex(Reference("v"), _), OutConn(), _),
          Edge(Reference("e2"), Vertex(Reference("v"), _), Vertex(Reference("w"), _), OutConn(), _))
          ) => }
        }
      }
    }

    // Ignored for now, because virtual paths are not supported.
    ignore("isObj = false for virtual path") {
      val ast = GcoreLang parseQuery
        "CONSTRUCT () " +
          "MATCH (u)-/p/->(v)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(
          MatchClause(
            /* non optional */ CondMatchClause(
              List(/*u-/p/->v =>*/ SimpleMatchClause(graphPattern, _)), /*where =*/ _),
            /* optional */ _
          )) => {

          graphPattern should matchPattern {
            case GraphPattern(List(Path(_, _, _, _, _, _, _, _, false))) => }
        }
      }
    }

    test("isObj = true for objectified path") {
      val ast = GcoreLang parseQuery
        "CONSTRUCT () " +
          "MATCH (u)-/@p/->(v)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(
          MatchClause(
            /* non optional */ CondMatchClause(
              List(/*u-/@p/->v =>*/ SimpleMatchClause(graphPattern, _)), /*where =*/ _),
            /* optional */ _)) => {

          graphPattern should matchPattern {
            case GraphPattern(List(Path(_, _, _, _, _, _, _, _, true))) => }
        }
      }
    }

    test("isReachableTest = true if name is not provided for path") {
      val ast = GcoreLang parseQuery "CONSTRUCT () MATCH (u)-/@/->(v)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(
          MatchClause(
            /* non optional */ CondMatchClause(
              List(/*u-/@/->v =>*/ SimpleMatchClause(graphPattern, _)), /*where =*/ _),
            /* optional */ _)) => {

          graphPattern should matchPattern {
            case GraphPattern(List(Path(_, true, _, _, _, _, _, _, _))) => }
        }
      }
    }

    test("isReachableTest = false if name is provided for path") {
      val ast = GcoreLang parseQuery "CONSTRUCT () MATCH (u)-/@p/->(v)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(
        MatchClause(
          /* non optional */ CondMatchClause(
            List(/*u-/@/->v =>*/ SimpleMatchClause(graphPattern, _)), /*where =*/ _),
          /* optional */ _)) => {

          graphPattern should matchPattern {
            case GraphPattern(List(Path(_, false, _, _, _, _, _, _, _))) => }
        }
      }
    }
  }

  def expressions(): Unit = {

    test("WHERE BasicGraphPattern => WHERE EXISTS (CONSTRUCT () MATCH BasicGraphPattern") {
      val ast = GcoreLang parseQuery "CONSTRUCT () MATCH (u) WHERE (u:Label)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(
          MatchClause(
            /* non optional */ CondMatchClause(/*simpleMatchClauses =*/ _, /*where =*/ expr),
            /* optional */ _)) =>

          expr should matchPattern { case Exists(Query(_)) => }
      }
    }

    test("WHERE NOT 2 => Not(2)") {
      val query = "CONSTRUCT () MATCH (u) WHERE NOT 2"
      val expected = Not(Literal(2, GcoreInteger()))
      runTest(query, expected)
    }

    test("WHERE -2 => Minus(2)") {
      val query = "CONSTRUCT () MATCH (u) WHERE -2"
      val expected = Minus(Literal(2, GcoreInteger()))
      runTest(query, expected)
    }

    test("WHERE 2 AND 2 => And(2, 2)") {
      val query = "CONSTRUCT () MATCH (u) WHERE 2 AND 2"
      val expected = And(lhs = Literal(2, GcoreInteger()), rhs = Literal(2, GcoreInteger()))
      runTest(query, expected)
    }

    test("WHERE 2 OR 2 => Or(2, 2)") {
      val query = "CONSTRUCT () MATCH (u) WHERE 2 OR 2"
      val expected = Or(lhs = Literal(2, GcoreInteger()), rhs = Literal(2, GcoreInteger()))
      runTest(query, expected)
    }

    test("WHERE 2 = 2 => Eq(2, 2)") {
      val query = "CONSTRUCT () MATCH (u) WHERE 2 = 2"
      val expected = Eq(lhs = Literal(2, GcoreInteger()), rhs = Literal(2, GcoreInteger()))
      runTest(query, expected)
    }

    test("WHERE 2 != 2 (Neq1) => Neq(2, 2)") {
      val query = "CONSTRUCT () MATCH (u) WHERE 2 != 2"
      val expected = Neq(lhs = Literal(2, GcoreInteger()), rhs = Literal(2, GcoreInteger()))
      runTest(query, expected)
    }

    test("WHERE 2 <> 2 (Neq2) => Neq(2, 2)") {
      val query = "CONSTRUCT () MATCH (u) WHERE 2 <> 2"
      val expected = Neq(lhs = Literal(2, GcoreInteger()), rhs = Literal(2, GcoreInteger()))
      runTest(query, expected)
    }

    test("WHERE 2 > 2 => Gt(2, 2)") {
      val query = "CONSTRUCT () MATCH (u) WHERE 2 > 2"
      val expected = Gt(lhs = Literal(2, GcoreInteger()), rhs = Literal(2, GcoreInteger()))
      runTest(query, expected)
    }

    test("WHERE 2 >= 2 => Gte(2, 2)") {
      val query = "CONSTRUCT () MATCH (u) WHERE 2 >= 2"
      val expected = Gte(lhs = Literal(2, GcoreInteger()), rhs = Literal(2, GcoreInteger()))
      runTest(query, expected)
    }

    test("WHERE 2 < 2 => Lt(2, 2)") {
      val query = "CONSTRUCT () MATCH (u) WHERE 2 < 2"
      val expected = Lt(lhs = Literal(2, GcoreInteger()), rhs = Literal(2, GcoreInteger()))
      runTest(query, expected)
    }

    test("WHERE 2 <= 2 => Lte(2, 2)") {
      val query = "CONSTRUCT () MATCH (u) WHERE 2 <= 2"
      val expected = Lte(lhs = Literal(2, GcoreInteger()), rhs = Literal(2, GcoreInteger()))
      runTest(query, expected)
    }

    test("WHERE 2^2 => Power(2, 2)") {
      val query = "CONSTRUCT () MATCH (u) WHERE 2^2"
      val expected = Power(lhs = Literal(2, GcoreInteger()), rhs = Literal(2, GcoreInteger()))
      runTest(query, expected)
    }

    test("WHERE 2*2 => Mul(2, 2)") {
      val query = "CONSTRUCT () MATCH (u) WHERE 2*2"
      val expected = Mul(lhs = Literal(2, GcoreInteger()), rhs = Literal(2, GcoreInteger()))
      runTest(query, expected)
    }

    test("WHERE 2/2 => Div(2, 2)") {
      val query = "CONSTRUCT () MATCH (u) WHERE 2/2"
      val expected = Div(lhs = Literal(2, GcoreInteger()), rhs = Literal(2, GcoreInteger()))
      runTest(query, expected)
    }

    test("WHERE 2%2 => Mod(2, 2)") {
      val query = "CONSTRUCT () MATCH (u) WHERE 2%2"
      val expected = Mod(lhs = Literal(2, GcoreInteger()), rhs = Literal(2, GcoreInteger()))
      runTest(query, expected)
    }

    test("WHERE 2+2 => Add(2, 2)") {
      val query = "CONSTRUCT () MATCH (u) WHERE 2+2"
      val expected = Add(lhs = Literal(2, GcoreInteger()), rhs = Literal(2, GcoreInteger()))
      runTest(query, expected)
    }

    test("WHERE 2-2 => Sub(2, 2)") {
      val query = "CONSTRUCT () MATCH (u) WHERE 2-2"
      val expected = Sub(lhs = Literal(2, GcoreInteger()), rhs = Literal(2, GcoreInteger()))
      runTest(query, expected)
    }

    test("WHERE 2 IS NULL => IsNull(2)") {
      val query = "CONSTRUCT () MATCH (u) WHERE 2 IS NULL"
      val expected = IsNull(Literal(2, GcoreInteger()))
      runTest(query, expected)
    }

    test("WHERE 2 IS NOT NULL => IsNotNull(2)") {
      val query = "CONSTRUCT () MATCH (u) WHERE 2 IS NOT NULL"
      val expected = IsNotNull(Literal(2, GcoreInteger()))
      runTest(query, expected)
    }

    test("WHERE u.prop => PropRef(u, prop)") {
      val query = "CONSTRUCT () MATCH (u) WHERE u.prop"
      val expected = PropertyRef(Reference("u"), PropertyKey("prop"))
      runTest(query, expected)
    }

    def runTest(query: String, expected: AlgebraExpression): Unit = {
      val ast = GcoreLang parseQuery query
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(
        MatchClause(
        /* non optional */ CondMatchClause(/*simpleMatchClauses =*/ _, /*where =*/ expr),
        /* optional */ _)) =>

          expr should matchPattern { case `expected` => }
      }
    }
  }

  def locations(): Unit = {

    test("DefaultGraph is used in SimpleMatchClause when no graph is specified.") {
      val ast = GcoreLang parseQuery
        "CONSTRUCT () " +
          "MATCH (u)"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(
          MatchClause(
            /* non optional */ CondMatchClause(List(SimpleMatchClause(_, graph)), _),
            /* optional */ _
          )) => {

          graph should matchPattern { case DefaultGraph() => }
        }
      }
    }

    test("NamedGraph is used in SimpleMatchClause when graph name is specified.") {
      val ast = GcoreLang parseQuery
        "CONSTRUCT () " +
          "MATCH (u) ON social_graph"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(
          MatchClause(
            /* non optional */ CondMatchClause(List(SimpleMatchClause(_, graph)), _),
            /* optional */_
          )) => {

          graph should matchPattern { case NamedGraph("social_graph") => }
        }
      }
    }

    test("QueryGraph is used in SimpleMatchClause when graph is provided through query.") {
      val ast = GcoreLang parseQuery
        "CONSTRUCT () " +
          "MATCH (u) ON (CONSTRUCT() MATCH (v))"
      val spoofaxTree = SpoofaxTreeBuilder build ast
      val algebraTree = AlgebraTreeBuilder build spoofaxTree

      inside (algebraTree) {
        case Query(
          MatchClause(
            /* non optional */ CondMatchClause(List(SimpleMatchClause(_, graph)), _),
            /* optional */ _
          )) => {

          graph should matchPattern { case QueryGraph(_) => }
        }
      }
    }
  }
}
