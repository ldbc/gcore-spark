package parser.trees

import algebra.expressions.Reference
import algebra.operators.{CondMatchClause, MatchClause, Query, SimpleMatchClause}
import algebra.trees.AlgebraTreeNode
import algebra.types._
import org.scalatest.{FunSuite, Inside, Matchers}

class MatchTreeBuilderTest extends FunSuite with Matchers with Inside with MinimalSpoofaxParser {

  /************************** Match mixing ********************************************************/
  test("Match with FullGraphPatternCondition and Optional clause") {
    val algebraTree = extractMatchClause("CONSTRUCT (u) MATCH (u) OPTIONAL (v)")

    algebraTree should matchPattern {
      case MatchClause(
      /*u =>*/ CondMatchClause(Seq(_: SimpleMatchClause), _),
      /*v =>*/ Seq(CondMatchClause(Seq(_: SimpleMatchClause), _)))=>
    }
  }

  /************************** Graph patterns ******************************************************/
  test("(u) => SimpleMatchClause([Vertex(u)])") {
    val algebraTree = extractMatchClause("CONSTRUCT (u) MATCH (u)")

    inside (algebraTree) {
      case MatchClause(
      /* non optional */ CondMatchClause(Seq(SimpleMatchClause(graphPattern, _)), _),
      /* optional */ _) =>

        graphPattern should matchPattern {
          case GraphPattern(
          /*topology =*/ Seq(Vertex(Reference("u"), _))) =>
        }
    }
  }

  test("(u), (v) => SimpleMatchClause([Vertex(u)]), SimpleMatchClause([Vertex(v)])") {
    val algebraTree = extractMatchClause("CONSTRUCT (u) MATCH (u), (v)")

    inside (algebraTree) {
      case MatchClause(
      /* non optional */ CondMatchClause(
      Seq(
      /*u =>*/ SimpleMatchClause(graphPatternU, _),
      /*v =>*/ SimpleMatchClause(graphPatternV, _)),
      /*where =*/ _),
      /* optional */ _) =>

        graphPatternU should matchPattern {
          case GraphPattern(
          /*topology =*/ Seq(Vertex(Reference("u"), _))) =>
        }

        graphPatternV should matchPattern {
          case GraphPattern(
          /*topology =*/ Seq(Vertex(Reference("v"), _))) =>
        }
    }
  }

  test("(u)->(v) => SimpleMatchClause([Edge(u->v)])") {
    val algebraTree = extractMatchClause("CONSTRUCT (u) MATCH (u)-[e]->(v)")

    inside (algebraTree) {
      case MatchClause(
      /* non optional */ CondMatchClause(
      Seq(
      /*u->v =>*/ SimpleMatchClause(graphPattern, _)),
      /*where =*/ _),
      /* optional */ _) =>

        graphPattern should matchPattern {
          case GraphPattern(
          /*topology =*/ Seq(Edge(
          /*connName =*/ Reference("e"),
          /*leftEndpoint =*/ Vertex(Reference("u"), _),
          /*rightEndpoint =*/ Vertex(Reference("v"), _),
          /*connType =*/ OutConn,
          /*expr =*/ _))) =>
        }
    }
  }

  test("(u)->(v), (v)->(w) => SimpleMatchClause([Edge(u->v)]), SimpleMatchClause([Edge(v->w)])") {
    val algebraTree = extractMatchClause("CONSTRUCT (u) MATCH (u)-[e1]->(v), (v)-[e2]->(w)")

    inside (algebraTree) {
      case MatchClause(
      /* non optional */ CondMatchClause(
      Seq(
      /*u->v =>*/ SimpleMatchClause(graphPattern1, _),
      /*v->w =>*/ SimpleMatchClause(graphPattern2, _)),
      /*where =*/ _),
      /* optional */ _) =>

        graphPattern1 should matchPattern {
          case GraphPattern(
          /*topology =*/ Seq(Edge(
          /*connName =*/ Reference("e1"),
          /*leftEndpoint =*/ Vertex(Reference("u"), _),
          /*rightEndpoint =*/ Vertex(Reference("v"), _),
          /*connType =*/ OutConn,
          /*expr =*/ _))) =>
        }

        graphPattern2 should matchPattern {
          case GraphPattern(
          /*topology =*/ Seq(Edge(
          /*connName =*/ Reference("e2"),
          /*leftEndpoint =*/ Vertex(Reference("v"), _),
          /*rightEndpoint =*/ Vertex(Reference("w"), _),
          /*connType =*/ OutConn,
          /*expr =*/ _))) =>
        }
    }
  }

  test("(u)->(v)->(w) => SimpleMatchClause([Edge(u->v), Edge(v->w)])") {
    val algebraTree = extractMatchClause("CONSTRUCT (u) MATCH (u)-[e1]->(v)-[e2]->(w)")

    inside (algebraTree) {
      case MatchClause(
      /* non optional */ CondMatchClause(
      Seq(
      /*u->v->w =>*/ SimpleMatchClause(graphPattern, _)),
      /*where =*/ _),
      /* optional */ _) =>

        graphPattern should matchPattern {
          case GraphPattern(Seq(
          Edge(Reference("e1"), Vertex(Reference("u"), _), Vertex(Reference("v"), _), OutConn, _),
          Edge(Reference("e2"), Vertex(Reference("v"), _), Vertex(Reference("w"), _), OutConn, _)))
          =>
        }
    }
  }

  // Ignored for now, because virtual paths are not supported.
  ignore("isObj = false for virtual path") {
    val algebraTree = extractMatchClause("CONSTRUCT (u) MATCH (u)-/p/->(v)")

    inside (algebraTree) {
      case MatchClause(
      /* non optional */ CondMatchClause(
      Seq(/*u-/p/->v =>*/ SimpleMatchClause(graphPattern, _)), /*where =*/ _),
      /* optional */ _) =>

        graphPattern should matchPattern {
          case GraphPattern(Seq(Path(_, _, _, _, _, _, _, _, false, _))) =>
        }
    }
  }

  test("isObj = true for objectified path") {
    val algebraTree = extractMatchClause("CONSTRUCT (u) MATCH (u)-/@p/->(v)")

    inside (algebraTree) {
      case MatchClause(
      /* non optional */ CondMatchClause(
      Seq(/*u-/@p/->v =>*/ SimpleMatchClause(graphPattern, _)), /*where =*/ _),
      /* optional */ _) =>

        graphPattern should matchPattern {
          case GraphPattern(Seq(Path(_, _, _, _, _, _, _, _, true, _))) =>
        }
    }
  }

  test("isReachableTest = true if name is not provided for path") {
    val algebraTree = extractMatchClause("CONSTRUCT (u) MATCH (u)-/@/->(v)")

    inside (algebraTree) {
      case MatchClause(
      /* non optional */ CondMatchClause(
      Seq(/*u-/@/->v =>*/ SimpleMatchClause(graphPattern, _)), /*where =*/ _),
      /* optional */ _) =>

        graphPattern should matchPattern {
          case GraphPattern(Seq(Path(_, true, _, _, _, _, _, _, _, _))) =>
        }
    }
  }

  test("isReachableTest = false if name is provided for path") {
    val algebraTree = extractMatchClause("CONSTRUCT (u) MATCH (u)-/@p/->(v)")

    inside (algebraTree) {
      case MatchClause(
      /* non optional */ CondMatchClause(
      Seq(/*u-/@/->v =>*/ SimpleMatchClause(graphPattern, _)), /*where =*/ _),
      /* optional */ _)=>

        graphPattern should matchPattern {
          case GraphPattern(Seq(Path(_, false, _, _, _, _, _, _, _, _))) =>
        }
    }
  }

  /****************************** Locations *******************************************************/
  test("DefaultGraph is used in SimpleMatchClause when no graph is specified.") {
    val algebraTree = extractMatchClause("CONSTRUCT (u) MATCH (u)")

    inside (algebraTree) {
      case MatchClause(
      /* non optional */ CondMatchClause(Seq(SimpleMatchClause(_, graph)), _),
      /* optional */ _) =>

        graph should matchPattern { case DefaultGraph => }
    }
  }

  test("NamedGraph is used in SimpleMatchClause when graph name is specified.") {
    val algebraTree = extractMatchClause("CONSTRUCT (u) MATCH (u) ON social_graph")

    inside (algebraTree) {
      case MatchClause(
      /* non optional */ CondMatchClause(Seq(SimpleMatchClause(_, graph)), _),
      /* optional */_) =>

        graph should matchPattern { case NamedGraph("social_graph") => }
    }
  }

  test("QueryGraph is used in SimpleMatchClause when graph is provided through query.") {
    val algebraTree = extractMatchClause("CONSTRUCT (u) MATCH (u) ON (CONSTRUCT(v) MATCH (v))")

    inside (algebraTree) {
      case MatchClause(
      /* non optional */ CondMatchClause(Seq(SimpleMatchClause(_, graph)), _),
      /* optional */ _) =>

        graph should matchPattern { case _: QueryGraph => }
    }
  }

  private def extractMatchClause(query: String): AlgebraTreeNode = {
    parse(query).asInstanceOf[Query].getMatchClause
  }
}
