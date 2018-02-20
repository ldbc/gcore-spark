package ir.trees.spoofax

import ir.rewriters.SpoofaxCanonicalRewriter
import ir.trees.SpoofaxTreeBuilder
import ir.trees.spoofax.StrategoHelper._
import ir.utils.VarBinder
import org.scalatest.{BeforeAndAfterEach, FunSuite}

trait RewriteMatchClause extends BeforeAndAfterEach with NodeChecker {
  this: FunSuite =>

  override protected def beforeEach(): Unit = {
    VarBinder.reset()
  }

  def bindUnnamedVariables(): Unit = {
    test("() => (v_0)") {
      val query = vertexTree(/*ref =*/ Option.empty)
      val tree = SpoofaxTreeBuilder build query
      val rewrite = SpoofaxCanonicalRewriter rewriteDown tree

      val some = rewrite.children.head
      val objMatchPattern = rewrite.children(1)
      val varDef = some.children.head
      val ref = varDef.children.head

      checkNode(rewrite, "Vertex", 2)
      checkNode(some, "Some", 1)
      checkNode(varDef, "VarDef", 1)
      checkLeafValue(ref, "v_0")

      checkNode(objMatchPattern, "ObjectMatchPattern", 2)
      checkNone(objMatchPattern.children.head)
      checkNone(objMatchPattern.children(1))
    }

    test("(n)->(m) => (n)-[e_0]->(m)") {
      val query = simpleConnTree("OutEdge")
      val tree = SpoofaxTreeBuilder build query
      val rewrite = SpoofaxCanonicalRewriter rewriteDown tree

      val conn = rewrite.children.head
      checkConn(conn, "OutConn", "e_0")
    }

    test("(n)<-(m) => (n)<-[e_0]-(m)") {
      val query = simpleConnTree("InEdge")
      val tree = SpoofaxTreeBuilder build query
      val rewrite = SpoofaxCanonicalRewriter rewriteDown tree

      val conn = rewrite.children.head
      checkConn(conn, "InConn", "e_0")
    }

    test("(n)<->(m) => (n)<-[e_0]->(m)") {
      val query = simpleConnTree("InOutEdge")
      val tree = SpoofaxTreeBuilder build query
      val rewrite = SpoofaxCanonicalRewriter rewriteDown tree

      val conn = rewrite.children.head
      checkConn(conn, "InOutEdge", "e_0")
    }

    test("(n)-(m) => (n)-[e_0]-(m)") {
      val query = simpleConnTree("UndirectedEdge")
      val tree = SpoofaxTreeBuilder build query
      val rewrite = SpoofaxCanonicalRewriter rewriteDown tree

      val conn = rewrite.children.head
      checkConn(conn, "UndirectedEdge", "e_0")
    }

    test("(n)-->(m) => (n)-[e_0]->(m)") {
      val query = doubleConnTree("OutConn")
      val tree = SpoofaxTreeBuilder build query
      val rewrite = SpoofaxCanonicalRewriter rewriteDown tree

      val conn = rewrite.children.head
      checkConn(conn, "OutConn", "e_0")
    }

    test("(n)<--(m) => (n)<-[e_0]-(m)") {
      val query = doubleConnTree("InConn")
      val tree = SpoofaxTreeBuilder build query
      val rewrite = SpoofaxCanonicalRewriter rewriteDown tree

      val conn = rewrite.children.head
      checkConn(conn, "InConn", "e_0")
    }

    test("(n)<-->(m) => (n)<-[e_0]->(m)") {
      val query = doubleConnTree("InOutEdge")
      val tree = SpoofaxTreeBuilder build query
      val rewrite = SpoofaxCanonicalRewriter rewriteDown tree

      val conn = rewrite.children.head
      checkConn(conn, "InOutEdge", "e_0")
    }

    test("(n)--(m) => (n)-[e_0]-(m)") {
      val query = doubleConnTree("UndirectedEdge")
      val tree = SpoofaxTreeBuilder build query
      val rewrite = SpoofaxCanonicalRewriter rewriteDown tree

      val conn = rewrite.children.head
      checkConn(conn, "UndirectedEdge", "e_0")
    }

    test("(n)-[]->(m) => (n)-[e_0]->(m)") {
      val query = fullConnTreeEdge(Option.empty, "OutConn")
      val tree = SpoofaxTreeBuilder build query
      val rewrite = SpoofaxCanonicalRewriter rewriteDown tree

      val conn = rewrite.children.head
      checkConn(conn, "OutConn", "e_0")
    }

    test("(n)<-[]-(m) => (n)<-[e_0]-(m)") {
      val query = fullConnTreeEdge(Option.empty, "InConn")
      val tree = SpoofaxTreeBuilder build query
      val rewrite = SpoofaxCanonicalRewriter rewriteDown tree

      val conn = rewrite.children.head
      checkConn(conn, "InConn", "e_0")
    }

    test("(n)<-[]->(m) => (n)<-[e_0]->(m)") {
      val query = fullConnTreeEdge(Option.empty, "InOutEdge")
      val tree = SpoofaxTreeBuilder build query
      val rewrite = SpoofaxCanonicalRewriter rewriteDown tree

      val conn = rewrite.children.head
      checkConn(conn, "InOutEdge", "e_0")
    }

    test("(n)-[]-(m) => (n)-[e_0]-(m)") {
      val query = fullConnTreeEdge(Option.empty, "UndirectedEdge")
      val tree = SpoofaxTreeBuilder build query
      val rewrite = SpoofaxCanonicalRewriter rewriteDown tree

      val conn = rewrite.children.head
      checkConn(conn, "UndirectedEdge", "e_0")
    }

    test("(n)-/ /->(m) => (n)-/ /->(m) (do not bind unnamed path)") {
      val query = fullConnTreePath(Option.empty, "OutConn")
      val tree = SpoofaxTreeBuilder build query
      val rewrite = SpoofaxCanonicalRewriter rewriteDown tree

      val conn = rewrite.children.head
      val path = conn.children.head.children.head
      val virtual = path.children.head
      val varDef = virtual.children(1)

      checkNone(varDef)
    }
  }
}
