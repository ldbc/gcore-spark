/*
 * gcore-spark is the reference implementation of the G-CORE graph query
 * language by the Linked Data Benchmark Council (LDBC) - ldbcouncil.org
 *
 * The copyrights of the source code in this file belong to:
 * - CWI (www.cwi.nl), 2017-2018
 *
 * This software is released in open source under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package parser.trees

import common.trees.TopDownRewriter
import parser.exceptions.QueryParseException
import parser.utils.{Stratego, VarBinder}

/**
  * A canonical rewriter over a [[SpoofaxBaseTreeNode]] to fill in missing terms that we will need
  * for future processing.
  */
object SpoofaxCanonicalRewriter extends TopDownRewriter[SpoofaxBaseTreeNode] {

  private val inOutConnections = List("InConn", "OutConn")
  private val inOutEdgeConnections = List("InEdge", "OutEdge")
  private val otherConnections = List("UndirectedEdge", "InOutEdge")

  /**
    * If this Vertex's VarDef child is not present, we add it to the tree. The rewrite rule is:
    * () => (v_xy)
    */
  private val varDefUnnamedNode: RewriteFuncType = {
    case vertex @ SpoofaxBaseTreeNode("Vertex") =>
      val varDef = vertex.children.head
      if (varDef.name == "Some")
        vertex
      else {
        // For the construct pattern it should actually be a VarRefDef, but we treat it the same in
        // ConstructTreeBuilder anyway.
        val newVarDef = newVarDefTree("v")
        vertex.children = List(newVarDef) ++ vertex.children.tail
        vertex
      }
  }

  /**
    * Creates a name for an unnamed stored path variable in a construct clause. The rewrite rule is:
    * ()->()      =>  ()-/@p_xy/->()
    * ()-->()     =>  ()-/@p_xy/->()
    * ()-/@/->()  =>  ()-/@p_xy/->()
    *
    * ()<-()      =>  ()<-/@p_xy/-()
    * ()<--()     =>  ()<-/@p_xy/-()
    * ()<-/@/-()  =>  ()<-/@p_xy/-()
    *
    * ()<->()     =>  ()<-/@p_xy/->()
    * ()<-->()    =>  ()<-/@p_xy/->()
    * ()<-/@/->() =>  ()<-/@p_xy/->()
    */
  private val varDefUnnamedPathObjectified: RewriteFuncType = {
    case path @ SpoofaxBaseTreeNode("PathObjectified") =>
      val varRefDef = path.children(1)
      // The objectified path lexical tree will also contain a most redundant node, "@", the head
      // of the children list of path. We do not add it to the resulting path.
      if (varRefDef.name == "Some") {
        val copyPattern = path.children(2)
        val objConstructPattern = path.children.last
        path.children = List(varRefDef, copyPattern, objConstructPattern)
      }
      else {
        val newVarRefDef = newVarRefDefTree("p")
        val copyPattern = path.children(2)
        val objConstructPattern = path.children.last
        path.children = List(newVarRefDef, copyPattern, objConstructPattern)
      }

      path
  }

  /**
    * Together with [[varDefUnnamedConn]] rewrites a connection with the following rules:
    * ()-()      =>  ()-[e_xy]-()
    * ()--()     =>  ()-[e_xy]-()
    * ()-[]-()   =>  ()-[e_xy]-()
    *
    * ()->()     =>  ()-[e_xy]->()
    * ()-->()    =>  ()-[e_xy]->()
    * ()-[]->()  =>  ()-[e_xy]->()
    *
    * ()<-()     =>  ()<-[e_xy]-()
    * ()<--()    =>  ()<-[e_xy]-()
    * ()<-[]-()  =>  ()<-[e_xy]-()
    *
    * ()<->()    =>  ()<-[e_xy]->()
    * ()<-->()   =>  ()<-[e_xy]->()
    * ()<-[]->() =>  ()<-[e_xy]->()
    */
  private val varDefUnnamedEdge: RewriteFuncType = {
    case edgePattern: SpoofaxTreeNode
      if edgePattern.name == "EdgeMatchPattern" || edgePattern.name == "EdgeConstructPattern" =>

      val varDef = edgePattern.children.head
      if (varDef.name == "Some")
        edgePattern
      else {
        // For the construct pattern it should actually be a VarRefDef, but we treat it the same in
        // ConstructTreeBuilder anyway.
        val newVarDef = newVarRefDefTree("e")
        edgePattern.children = List(newVarDef) ++ edgePattern.children.tail
        edgePattern
      }
  }

  /**
    * @see [[varDefUnnamedEdge]]
    */
  private val varDefUnnamedConn: RewriteFuncType = {
    case matchPattern @ SpoofaxBaseTreeNode("EdgeVertexMatchPattern") =>
      val conn = matchPattern.children.head
      val vertex = matchPattern.children.last
      val newConn =
        rewriteConnection(
          matchPattern.children.head,
          newEdgeMatchPatternTree,
          newConnectionMatchTree)
      matchPattern.children = List(newConn, vertex)

      // If this is a connection with pattern <-[]->, then the direct child of the InOutEdge will be
      // Some->EdgeMatchPattern, instead of Some->Edge->EdgeMatchPattern. We add the missing Edge
      // node here.
      val patternWithEdge = addEdgeToInOutEdge(matchPattern)

      patternWithEdge

    case constructPattern @ SpoofaxBaseTreeNode("EdgeVertexConstructPattern") =>
      val conn = constructPattern.children.head
      val vertex = constructPattern.children.last

      // In case we are missing nodes from this connection, we add them here.
      val newConn =
        rewriteConnection(
          constructPattern.children.head,
          newEdgeConstructTree,
          newConnectionConstructTree)
      constructPattern.children = List(newConn, vertex)

      // In case we had the necessary nodes in this connection, we check whether the node
      // EdgeConstructPattern is missing or is called Edge instead and fix this.
      val constructPatternRenamed = addEdgeConstructPatternToEdge(constructPattern)

      constructPatternRenamed
  }

  private def addEdgeToInOutEdge(pattern: SpoofaxBaseTreeNode): SpoofaxBaseTreeNode = {
    val connection = pattern.children.head
    connection.name match {
      case "InOutEdge" =>
        val some = connection.children.head
        if (some.children.head.name == "EdgeMatchPattern") {
          val edge = SpoofaxTreeNode(Stratego.edge(Stratego.none))
          edge.children = some.children
          some.children = List(edge)
        }

        pattern

      case _ => pattern
    }
  }

  private def addEdgeConstructPatternToEdge(pattern: SpoofaxBaseTreeNode): SpoofaxBaseTreeNode = {
    val connection = pattern.children.head
    connection.name match {
      case "InOutEdge" =>
        val some = connection.children.head
        if (some.name == "Some" && some.children.head.name == "Edge") {
          val edge = some.children.head
          val newEdge =
            SpoofaxTreeNode(
              Stratego.edge(
                Stratego.edgeConstructPattern(
                  Stratego.none,
                  Stratego.none,
                  Stratego.none,
                  Stratego.none)))
          val newConstructPattern = newEdge.children.head
          newConstructPattern.children = edge.children
          some.children = List(newEdge)
        }

        pattern

      case _ =>
        val some = connection.children.head
        val edge = some.children.head
        val constructPattern = edge.children.head
        if (constructPattern.name == "Edge") {
          val newConstructPattern =
            SpoofaxTreeNode(
              Stratego.edgeConstructPattern(
                Stratego.none,
                Stratego.none,
                Stratego.none,
                Stratego.none))
          newConstructPattern.children = constructPattern.children
          edge.children = List(newConstructPattern)
        }

        pattern
    }
  }

  private
  def rewriteConnection(conn: SpoofaxBaseTreeNode,
                        newEdgeTree: => SpoofaxBaseTreeNode,
                        newConnectionTree: String => SpoofaxTreeNode): SpoofaxTreeNode = {
    conn match {
      case conn: SpoofaxTreeNode if inOutConnections.contains(conn.name) =>
        val edgePattern = conn.children.head
        if (edgePattern.name == "Some")
          conn
        else {
          conn.children = List(newEdgeTree)
          conn
        }
      case conn: SpoofaxTreeNode if inOutEdgeConnections.contains(conn.name) =>
        conn.name match {
          case "InEdge" => newConnectionTree("InConn")
          case "OutEdge" => newConnectionTree("OutConn")
        }
      case conn: SpoofaxTreeNode if otherConnections.contains(conn.name) =>
        if (conn.children.nonEmpty && conn.children.head.name == "Some") // at least one child
          conn // child was "Some", good, can move on
        else // either "None" or no children, bad, need to name this connection
          newConnectionTree(conn.name)
      case _ =>
        throw QueryParseException(s"Unknown connection ${conn.name}.")
    }
  }

  private def newVarDefTree(prefix: String): SpoofaxTreeNode = {
    SpoofaxTreeNode(
      Stratego.some(
        Stratego.varDef(
          Stratego.string(VarBinder.createVar(prefix)))))
  }

  private def newVarRefDefTree(prefix: String): SpoofaxTreeNode = {
    SpoofaxTreeNode(
      Stratego.some(
        Stratego.varRefDef(
          Stratego.string(VarBinder.createVar(prefix)))))
  }

  private def newEdgeMatchPatternTree: SpoofaxTreeNode = {
    SpoofaxTreeNode(
      Stratego.some(
        Stratego.edge(
          Stratego.edgeMatchPattern(
            Stratego.none,
            Stratego.objectMatchPattern(Stratego.none, Stratego.none)))))
  }

  private def newEdgeConstructTree: SpoofaxTreeNode = {
    SpoofaxTreeNode(
      Stratego.some(
        Stratego.edge(
          Stratego.edgeConstructPattern(
            /*varRefDef =*/ Stratego.none,
            /*copyPattern =*/ Stratego.none,
            /*groupDeclaration =*/ Stratego.none,
            /*objConstrPattern =*/ Stratego.objectConstructPattern(Stratego.none, Stratego.none))))
    )
  }

  private def newConnectionMatchTree(name: String): SpoofaxTreeNode = {
    SpoofaxTreeNode(
      Stratego.conn(
        name,
        Stratego.some(
          Stratego.edge(
            Stratego.edgeMatchPattern(
              Stratego.none,
              Stratego.objectMatchPattern(Stratego.none, Stratego.none))))))
  }

  private def newConnectionConstructTree(name: String): SpoofaxTreeNode = {
    SpoofaxTreeNode(
      Stratego.conn(
        name,
        Stratego.some(
          Stratego.edge(
            Stratego.edgeConstructPattern(
              /*varRefDef =*/ Stratego.none,
              /*copyPattern =*/ Stratego.none,
              /*groupDeclaration =*/ Stratego.none,
              /*objConstrPattern =*/ Stratego.objectConstructPattern(Stratego.none, Stratego.none))
          )
        )
      )
    )
  }

  override val rule: RewriteFuncType =
    varDefUnnamedNode orElse
      varDefUnnamedEdge orElse
      varDefUnnamedConn orElse
      varDefUnnamedPathObjectified
}
