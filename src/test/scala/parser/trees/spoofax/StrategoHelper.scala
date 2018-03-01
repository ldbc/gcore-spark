package parser.trees.spoofax

import parser.utils.Stratego
import org.spoofax.interpreter.terms.IStrategoTerm

object StrategoHelper {

  /** A Spoofax Match node. */
  def matchTree(connections: Seq[IStrategoTerm] = List(vertexTree(Option.empty)),
                location: IStrategoTerm = Stratego.none,
                where: IStrategoTerm = Stratego.none,
                optionals: IStrategoTerm = Stratego.none): IStrategoTerm = {
    Stratego.matchClause(
      Stratego.fullGraphPatternCondition(
        Stratego.fullGraphPattern(
          List(
            Stratego.basicGraphPatternLocation(
              Stratego.basicGraphPattern(
                /*vertexMatchPattern =*/ connections.head,
                /*edgeVertexMatchPatterns =*/ connections.tail),
              /*location =*/ location))),
        /*where =*/ where),
      /*optionals =*/ optionals)
  }

  /** A Spoofax Vertex node. */
  def vertexTree(ref: Option[String],
                 labels: IStrategoTerm = Stratego.none,
                 properties: IStrategoTerm = Stratego.none): IStrategoTerm = {
    Stratego.vertexMatchPattern(
      someVarDefOrNone(ref),
      Stratego.objectMatchPattern(labels, properties))
  }

  /** For OutEdge, InEdge, InOutEdge, UndirectedEdge, connections of type: (n)-(m) */
  def simpleConnTree(connType: String,
                     rightEndpoint: IStrategoTerm = vertexTree(Option("v"))): IStrategoTerm = {
    Stratego.edgeVertexMatchPattern(
      Stratego.conn(connType),
      rightEndpoint)
  }

  /** For OutConn, InConn, InOutEdge, UndirectedEdge. */
  def doubleConnTree(connType: String,
                     rightEndpoint: IStrategoTerm = vertexTree(Option("v"))): IStrategoTerm = {
    Stratego.edgeVertexMatchPattern(
      Stratego.conn(connType, Stratego.none),
      rightEndpoint)
  }

  /** For OutConn, InConn, InOutEdge, UndirectedEdge. */
  def fullConnTreeEdge(ref: Option[String],
                       connType: String,
                       rightEndpoint: IStrategoTerm = vertexTree(Option("v")),
                       labels: IStrategoTerm = Stratego.none,
                       properties: IStrategoTerm = Stratego.none): IStrategoTerm = {
    Stratego.edgeVertexMatchPattern(
      Stratego.conn(
        connType,
        Stratego.some(
          Stratego.edge(
            Stratego.edgeMatchPattern(
              someVarDefOrNone(ref),
              Stratego.objectMatchPattern(labels, properties))))),
      rightEndpoint)
  }

  def fullConnTreePath(ref: Option[String],
                       connType: String,
                       rightEndpoint: IStrategoTerm = vertexTree(Option("v"))): IStrategoTerm = {
    Stratego.edgeVertexMatchPattern(
      Stratego.conn(
        connType,
        Stratego.some(
          Stratego.path(Stratego.vpath(
            /*pathQuantifier =*/ Stratego.none,
            /*varDef =*/ someVarDefOrNone(ref),
            /*pathExpression =*/ Stratego.none,
            /*costVarDef =*/ Stratego.none)))),
      rightEndpoint)
  }

  private def someVarDefOrNone(ref: Option[String]): IStrategoTerm = {
    if (ref.isEmpty)
      Stratego.none
    else
      Stratego.some(Stratego.varDef(Stratego.string(ref.get)))
  }
}
