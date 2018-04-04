package algebra.exceptions

import algebra.expressions.{Label, PropertyKey, Reference}
import algebra.types.DoubleEndpointConn
import schema.EntitySchema

abstract class SemanticException(reason: String) extends AlgebraException(reason)

case class NamedGraphNotAvailableException(graphName: String)
  extends SemanticException(s"Graph $graphName is not available.")

case class DefaultGraphNotAvailableException()
  extends SemanticException("No default graph available.")

case class AmbiguousGraphForExistentialPatternException(graphName1: String, graphName2: String,
                                                        conn: DoubleEndpointConn)
  extends SemanticException(
    s"Ambiguous graph in existential pattern: either $graphName1 or $graphName2 in connection " +
      s"{${conn.getLeftEndpoint.getRef}, ${conn.getRef}, ${conn.getRightEndpoint.getRef}}.")

case class DisjunctLabelsException(graphName: String,
                                   unavailableLabels: Seq[Label],
                                   schema: EntitySchema)
  extends SemanticException(
    s"The following labels are either not available in the graph $graphName or have been use " +
      s"with the wrong entity type: ${unavailableLabels.map(_.value).mkString(", ")}.\n " +
      s"Entity schema is:\n$schema")

case class PropKeysException(graphName: String,
                             unavailableProps: Seq[PropertyKey],
                             schema: EntitySchema)
  extends SemanticException(
    s"The following property keys are mis-associated with their entity in graph $graphName: " +
      s"${unavailableProps.map(_.key).mkString(", ")}.\n " +
      s"Entity schema is:\n$schema")

case class JoinException(lhsBset: Set[Reference], rhsBset: Set[Reference])
  extends SemanticException(
    s"Cannot join relations with no common attributes. Left attributes are: $lhsBset, right " +
      s"attributes are $rhsBset")

case class CrossJoinException(lhsBset: Set[Reference], rhsBset: Set[Reference])
  extends SemanticException(
    s"Cannot cross-join relations with common attributes. Left attributes are: $lhsBset, right " +
      s"attributes are $rhsBset")
