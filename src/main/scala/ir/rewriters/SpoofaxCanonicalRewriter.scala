package ir.rewriters

import ir.trees.{SpoofaxBaseTreeNode, SpoofaxTreeNode}
import ir.utils.{Stratego, VarBinder}

/**
  * A canonical rewriter over a [[SpoofaxBaseTreeNode]] to fill in missing terms that we will need
  * for future processing.
  */
object SpoofaxCanonicalRewriter extends Rewriter[SpoofaxBaseTreeNode] {

  override def rule: PartialFunction[SpoofaxBaseTreeNode, SpoofaxBaseTreeNode] =
    varDefUnnamedNode orElse varDefUnnamedEdge orElse varDefUnnamedConn

  private val inOutConnections = List("InConn", "OutConn")
  private val inOutEdgeConnections = List("InEdge", "OutEdge")
  private val otherConnections = List("UndirectedEdge", "InOutEdge")

  /**
    * If this Vertex's VarDef child is not present, we add it to the tree. The rewrite rule is:
    * () => (v_xy)
    */
  private val varDefUnnamedNode: PartialFunction[SpoofaxBaseTreeNode, SpoofaxBaseTreeNode] = {
    case vertex: SpoofaxTreeNode if vertex.name == "Vertex" => {
      val varDef = vertex.children.head
      val objMatchPattern = vertex.children(1)
      if (varDef.name == "Some")
        vertex
      else {
        val newVarDef = newVarDefTree("v")
        vertex.children = List(newVarDef, objMatchPattern)
        vertex
      }
    }
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
  private val varDefUnnamedEdge: PartialFunction[SpoofaxBaseTreeNode, SpoofaxBaseTreeNode] = {
    case edgeMatchPattern: SpoofaxTreeNode if edgeMatchPattern.name == "EdgeMatchPattern" => {
      val varDef = edgeMatchPattern.children.head
      val objMatchPattern = edgeMatchPattern.children(1)
      if (varDef.name == "Some")
        edgeMatchPattern
      else {
        val newVarDef = newVarDefTree("e")
        edgeMatchPattern.children = List(newVarDef, objMatchPattern)
        edgeMatchPattern
      }
    }
  }


  /**
    * @see [[varDefUnnamedEdge]]
    */
  private val varDefUnnamedConn: PartialFunction[SpoofaxBaseTreeNode, SpoofaxBaseTreeNode] = {
    case conn: SpoofaxTreeNode if inOutConnections.contains(conn.name) => {
      val edgeMatchPattern = conn.children.head
      if (edgeMatchPattern.name == "Some")
        conn
      else {
        val newEdgeMatchPattern = newEdgeMatchPatternTree
        conn.children = List(newEdgeMatchPattern)
        conn
      }
    }
    case conn: SpoofaxTreeNode if inOutEdgeConnections.contains(conn.name) => {
      conn.name match {
        case "InEdge" => newConnectionTree("InConn")
        case "OutEdge" => newConnectionTree("OutConn")
      }
    }
    case conn: SpoofaxTreeNode if otherConnections.contains(conn.name) => {
      if (conn.children.nonEmpty && conn.children.head.name == "Some") // at least one child
        conn // child was "Some", good, can move on
      else   // either "None" or no children, bad, need to name this connection
        newConnectionTree(conn.name)
    }
  }

  private def newVarDefTree(prefix: String): SpoofaxTreeNode = {
    SpoofaxTreeNode(
      Stratego.some(
        Stratego.varDef(
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

  private def newConnectionTree(name: String): SpoofaxTreeNode = {
    SpoofaxTreeNode(
      Stratego.conn(
        name,
        Stratego.some(
          Stratego.edge(
            Stratego.edgeMatchPattern(
              Stratego.none,
              Stratego.objectMatchPattern(Stratego.none, Stratego.none))))))
  }
}
