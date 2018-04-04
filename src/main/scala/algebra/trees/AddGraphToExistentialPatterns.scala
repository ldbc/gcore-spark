package algebra.trees

import algebra.exceptions.AmbiguousGraphForExistentialPatternException
import algebra.expressions.{Exists, Reference}
import algebra.operators.SimpleMatchClause
import algebra.types._
import common.trees.TopDownRewriter

case class AddGraphToExistentialPatterns(context: AlgebraContext)
  extends TopDownRewriter[AlgebraTreeNode] {

  assert(context.bindingToGraph.isDefined,
    "The bindingToGraph field of the AlgebraContext must be set for this phase.")

  val bindingToGraph: Map[Reference, Graph] = context.bindingToGraph.get

  override val rule: RewriteFuncType = {
    case existsClause: Exists =>
      val simpleMatches: Seq[SimpleMatchClause] =
        existsClause.graphPattern.children
          .map {
            case v: SingleEndpointConn =>
              Tuple2(v, bindingToGraph.getOrElse(v.getRef, DefaultGraph()))

            case ep: DoubleEndpointConn =>
              val leftEndpGraph: Graph =
                bindingToGraph.getOrElse(key = ep.getLeftEndpoint.getRef, default = DefaultGraph())
              val rightEndpGraph: Graph =
                bindingToGraph.getOrElse(key = ep.getRightEndpoint.getRef, default = DefaultGraph())
              val connGraph: Graph =
                bindingToGraph.getOrElse(key = ep.getRef, default = DefaultGraph())

              val epGraph: Graph =
                Seq(leftEndpGraph, rightEndpGraph, connGraph)
                  .reduceLeft((graph1: Graph, graph2: Graph) => {
                    (graph1, graph2) match {
                      case (ng: NamedGraph, DefaultGraph()) => ng
                      case (DefaultGraph(), ng: NamedGraph) => ng
                      case (ng1: NamedGraph, ng2: NamedGraph) =>
                        if (ng1.graphName != ng2.graphName)
                          throw AmbiguousGraphForExistentialPatternException(
                            ng1.graphName, ng2.graphName, ep)
                        else ng1
                      case _ => DefaultGraph()
                    }
                  })

              Tuple2(ep, epGraph)
          }
          .map(connToGraph => SimpleMatchClause(GraphPattern(Seq(connToGraph._1)), connToGraph._2))

      existsClause.children = simpleMatches
      existsClause
  }
}
