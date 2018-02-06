package ir.rewriters

import ir.trees.{SpoofaxBaseTreeNode, SpoofaxTreeNode}
import org.spoofax.interpreter.terms.IStrategoTerm
import org.spoofax.terms.{StrategoAppl, StrategoConstructor, StrategoString}

object SpoofaxCanonicalRewriter extends Rewriter[SpoofaxBaseTreeNode] {

  override def rule: PartialFunction[SpoofaxBaseTreeNode, SpoofaxBaseTreeNode] =
    varDefUnnamedNode orElse varDefUnnamedEdge orElse varDefUnnamedConn orElse varDefUnnamedPath

  private var unnamedVarId: Int = -1
  private val inOutConnections = List("InConn", "OutConn")
  private val inOutEdgeConnections = List("InEdge", "OutEdge")
  private val otherConnections = List("UndirectedEdge", "InOutEdge")

  val varDefUnnamedNode: PartialFunction[SpoofaxBaseTreeNode, SpoofaxBaseTreeNode] = {
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

  val varDefUnnamedEdge: PartialFunction[SpoofaxBaseTreeNode, SpoofaxBaseTreeNode] = {
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

  val varDefUnnamedPath: PartialFunction[SpoofaxBaseTreeNode, SpoofaxBaseTreeNode] = {
    case pathType: SpoofaxTreeNode if pathType.name == "Virtual" => {
      val pathQuantifier = pathType.children.head
      val varDef = pathType.children(1)
      val pathExpr = pathType.children(2)
      val costVarDef = pathType.children(3)
      if (varDef.name == "Some")
        pathType
      else {
        val newVarDef = newVarDefTree("p")
        pathType.children = List(pathQuantifier, newVarDef, pathExpr, costVarDef)
        pathType
      }
    }
    case pathType: SpoofaxTreeNode if pathType.name == "Objectified" => {
      val pathQuantifier = pathType.children.head
      val varDef = pathType.children(2)
      val pathExpr = pathType.children(3)
      var objMatchPattern = pathType.children(4)
      val costVarDef = pathType.children(5)
      if (varDef.name == "Some") {
        pathType.children = List(pathQuantifier, varDef, pathExpr, objMatchPattern, costVarDef)
        pathType
      } else {
        val newVarDef = newVarDefTree("p")
        pathType.children = List(pathQuantifier, newVarDef, pathExpr, objMatchPattern, costVarDef)
        pathType
      }
    }
  }

  val varDefUnnamedConn: PartialFunction[SpoofaxBaseTreeNode, SpoofaxBaseTreeNode] = {
    case conn: SpoofaxTreeNode if inOutConnections.contains(conn.name) => {
      val edgeMatchPattern = conn.children.head
      if (edgeMatchPattern.name == "Some")
        edgeMatchPattern
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
          Stratego.string(createVar(prefix)))))
  }

  private def newEdgeMatchPatternTree: SpoofaxTreeNode = {
    SpoofaxTreeNode(
      Stratego.some(
        Stratego.edge(
          Stratego.edgeMatchPattern(Stratego.none, Stratego.none))))
  }

  private def newConnectionTree(name: String): SpoofaxTreeNode = {
    SpoofaxTreeNode(
      Stratego.conn(
        name,
        Stratego.some(
          Stratego.edge(
            Stratego.edgeMatchPattern(Stratego.none, Stratego.none)))))
  }


  private def createVar(prefix: String): String = {
    unnamedVarId += 1
    prefix + "_" + unnamedVarId
  }
}

object Stratego {
  def objectMatchPattern(labelPreds: IStrategoTerm, propsPreds: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "ObjectMatchPattern",
        /*arity = */ 2),
      /*kids = */ Array(labelPreds, propsPreds),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def none: IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "None",
        /*arity = */ 0),
      /*kids = */ Array.empty[IStrategoTerm],
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def some(child: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "Some",
        /*arity = */ 1),
      /*kids = */ Array(child),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def varDef(str: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "VarDef",
        /*arity = */ 1),
      /*kids = */ Array(str),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def string(value: String): IStrategoTerm = {
    new StrategoString(
      /*value = */ value,
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def edge(edgeMatchPattern: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "Edge",
        /*arity = */ 1),
      /*kids = */ Array(edgeMatchPattern),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def edgeMatchPattern(varDef: IStrategoTerm, objMatchPattern: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "EdgeMatchPattern",
        /*arity = */ 2),
      /*kids = */ Array(varDef, objMatchPattern),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def conn(name: String, child: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ name,
        /*arity = */ 1),
      /*kids = */ Array(child),
      /*annotations = */ null,
      /*storageType = */ 0)
  }
}
