package algebra.trees

import algebra.expressions.Reference
import algebra.types.{Edge, Path, Vertex}

import scala.collection.mutable

/**
  * Traverses an algebraic tree and extracts all vertex, edge and path references found in that
  * tree. The result is wrapped into a [[BindingContext]].
  */
object ExtractReferenceTuples {

  def extractReferenceTuples(algebraTreeNode: AlgebraTreeNode): BindingContext = {
    val vertexTuples: mutable.ArrayBuffer[Reference] = mutable.ArrayBuffer[Reference]()
    val edgeTuples: mutable.ArrayBuffer[ReferenceTuple] = mutable.ArrayBuffer[ReferenceTuple]()
    val pathTuples: mutable.ArrayBuffer[ReferenceTuple] = mutable.ArrayBuffer[ReferenceTuple]()

    algebraTreeNode.forEachDown {
      case vertex: Vertex => vertexTuples += vertex.getRef
      case edge: Edge =>
        edgeTuples +=
          ReferenceTuple(edge.getRef, edge.getLeftEndpoint.getRef, edge.getRightEndpoint.getRef)
      case path: Path =>
        pathTuples +=
          ReferenceTuple(path.getRef, path.getLeftEndpoint.getRef, path.getRightEndpoint.getRef)
      case _ =>
    }

    BindingContext(vertexTuples.toSet, edgeTuples.toSet, pathTuples.toSet)
  }
}
