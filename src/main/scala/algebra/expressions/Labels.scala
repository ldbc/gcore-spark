package algebra.expressions

import algebra.exceptions.DisjunctLabelsException
import algebra.trees.{DisjunctLabelsContext, SemanticCheck, SemanticCheckWithContext}
import common.compiler.Context
import common.exceptions.UnsupportedOperation

/**
  * All graph entities (vertices, edges and paths) must be labeled. The label then acts as the type
  * of the particular entity and also as an umbrella for a set of properties.
  */
case class Label(value: String) extends AlgebraExpression {
  children = List(StringLiteral(value))

  override def name: String = value
}

/**
  * An [[ObjectPattern]] predicate that restricts a variable to a set of disjunct [[Label]]s. The
  * variable satisfies the pattern if it is labeled with either of the [[Label]]s in the set.
  */
case class DisjunctLabels(labels: Seq[Label]) extends AlgebraExpression
  with SemanticCheck with SemanticCheckWithContext {

  children = labels

  /**
    * Check that the [[Label]]s used in the predicate are (1) available in the graph and (2) used
    * with the correct entity type of the variable they define. For example, a [[Label]] assigned to
    * a vertex in the stored graph cannot be used to determine an edge in the query.
    */
  override def checkWithContext(context: Context): Unit = {
    val schema = context.asInstanceOf[DisjunctLabelsContext].schema
    val availableLabels = schema.labels
    val givenLabels = labels
    val givenUnavailable = givenLabels filterNot availableLabels.contains
    if (givenUnavailable.nonEmpty)
      throw
        DisjunctLabelsException(
          graphName = context.asInstanceOf[DisjunctLabelsContext].graphName,
          unavailableLabels = givenUnavailable,
          schema = schema)
  }

  /** Label disjunction is not enabled in the current version of the interpreter. */
  override def check(): Unit = {
    if (labels.lengthCompare(1) > 0)
      throw UnsupportedOperation("Label disjunction is not supported. An entity must have " +
        "only one label associated with it.")
  }
}

/**
  * An [[ObjectPattern]] predicate that restricts a variable to a conjunction of sets of disjunct
  * labels. The variable satisfies the pattern if it is labeled with at least one [[Label]] from
  * each set participating in the conjunction.
  *
  * For example, the following labeling rule
  *
  * :foo|bar|baz:fred:qux
  *
  * is satisfied if the variable it is applied on is labeled with "fred" and "qux" and at least one
  * of the labels "foo", "bar" or "baz". In this case, the algebraic subtree is:
  *
  * ConjunctLabels(
  *   And(
  *     DisjunctLabels(foo, bar, baz),
  *     And(
  *       DisjunctLabels(fred)
  *       And(DisjunctLabels(qux), True)
  *     )
  *   )
  * )
  */
case class ConjunctLabels(labelConj: AlgebraExpression)
  extends AlgebraExpression with SemanticCheck {

  children = List(labelConj)

  /** Label conjunction is not enabled in the current version of the interpreter. */
  override def check(): Unit =
    labelConj match {
      case And(_: DisjunctLabels, True) =>
      case _ =>
        throw UnsupportedOperation("Label conjunction is not supported. An entity must have " +
          "only one label associated with it.")
    }
}

/**
  * An [[ObjectConstructPattern]] member that creates new labels for a given constructed variable.
  */
case class LabelAssignments(labels: Seq[Label]) extends AlgebraExpression {

  children = labels

  /**
    * Creates a new [[LabelAssignments]] from the set union of this object's and other's [[labels]].
    */
  def merge(other: LabelAssignments): LabelAssignments =
    LabelAssignments((this.labels.toSet ++ other.labels.toSet).toSeq)
}

/** Updates the graph by removing a label from a variable. */
case class LabelRemove(ref: Reference, labelAssignments: LabelAssignments)
  extends AlgebraExpression {

  children = List(ref, labelAssignments)
}
