package algebra.trees

import algebra.expressions.{Exists, Label, PropertyKey, Reference}
import algebra.operators.SimpleMatchClause
import algebra.types._

import scala.collection.mutable

case class MapBindingToGraph(context: AlgebraContext) {

  def mapBindingToGraph(tree: AlgebraTreeNode): Map[Reference, Graph] = {
    val simpleMatchClauseMapping: Map[Reference, Graph] = bindingsInSimpleMatchClause(tree)
    val existsMapping: Map[Reference, Graph] = bindingsInExists(tree, simpleMatchClauseMapping)

    simpleMatchClauseMapping ++ existsMapping
  }

  private def bindingsInSimpleMatchClause(tree: AlgebraTreeNode): Map[Reference, Graph] = {
    val buffer: mutable.ArrayBuffer[(Reference, Graph)] =
      new mutable.ArrayBuffer[(Reference, Graph)]()

    // Traverse the algebraic tree and, for each SimpleMatchClause, add the mapping from its
    // reference (and possibly endpoint references) to the graph it's defined on.
    tree.forEachDown {
      case SimpleMatchClause(graphPattern, graph) =>
        graphPattern.children foreach {
          case v: SingleEndpointConn => buffer += Tuple2(v.getRef, graph)
          case ep: DoubleEndpointConn =>
            buffer += Tuple2(ep.getLeftEndpoint.getRef, graph)
            buffer += Tuple2(ep.getRef, graph)
            buffer += Tuple2(ep.getRightEndpoint.getRef, graph)
        }
      case _ =>
    }

    // TODO: We should check here that no binding is mapped to more than one graph.
    buffer.toMap
  }

  private def bindingsInExists(tree: AlgebraTreeNode,
                               knownMapping: Map[Reference, Graph]): Map[Reference, Graph] = {
    val buffer: mutable.ArrayBuffer[(Reference, Graph)] =
      new mutable.ArrayBuffer[(Reference, Graph)]()

    tree.forEachDown {
      case Exists(graphPattern) =>
        graphPattern.children.foreach {
          // If a given binding has not been already mapped, we try to map it based on its
          // labels or properties within the ObjectPattern.
          case c: Connection if !knownMapping.isDefinedAt(c.getRef) =>
            c.getExpr.forEachDown {
              case l: Label =>
                context.graphDb.allGraphs.foreach(graph => {
                  if (graph.vertexSchema.labels.contains(l))
                    buffer += Tuple2(c.getRef, NamedGraph(graph.graphName))
                  if (graph.edgeSchema.labels.contains(l))
                    buffer += Tuple2(c.getRef, NamedGraph(graph.graphName))
                  if (graph.pathSchema.labels.contains(l))
                    buffer += Tuple2(c.getRef, NamedGraph(graph.graphName))
                })

              case pk: PropertyKey =>
                context.graphDb.allGraphs.foreach(graph => {
                  if (graph.vertexSchema.properties.contains(pk))
                    buffer += Tuple2(c.getRef, NamedGraph(graph.graphName))
                  if (graph.edgeSchema.properties.contains(pk))
                    buffer += Tuple2(c.getRef, NamedGraph(graph.graphName))
                  if (graph.pathSchema.properties.contains(pk))
                    buffer += Tuple2(c.getRef, NamedGraph(graph.graphName))
                })

              case _ =>
            }
        }

      case _ =>
    }

    // TODO: We should check here that no binding is mapped to more than one graph.
    buffer.toMap
  }
}
