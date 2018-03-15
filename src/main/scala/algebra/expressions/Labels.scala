package algebra.expressions

import algebra.exceptions.{DisjunctLabelsException, UnsupportedOperation}
import algebra.trees.{DisjunctLabelsContext, SemanticCheck, SemanticCheckWithContext}
import common.compiler.Context

case class Label(value: String) extends AlgebraExpression {
  children = List(Literal(value))

  def this(literal: Literal[String]) = this(literal.literalValue)

  override def name: String = value
}

/**
  * A predicate that asserts that the graph entity has at least one of the given labels. It is a
  * disjunction of labels. The labels are expressed through their [[Literal]] value.
  */
case class HasLabel(labels: Seq[Label]) extends AlgebraExpression with SemanticCheckWithContext {
  children = labels

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
}

/**
  * A predicate that asserts that the graph entity has all the given labels. It is a conjunction of
  * (disjunctions of) labels.
  *
  * For example, the following labeling rule
  *
  * :foo|bar|baz:fred:qux
  *
  * would become:
  *
  * WithLabels(
  *   And(
  *     HasLabel(foo, bar, baz),
  *     And(
  *       HasLabel(fred)
  *       And(HasLabel(qux), True)
  *     )
  *   )
  * )
  */
case class WithLabels(labelConj: AlgebraExpression) extends PredicateExpression(labelConj)
  with SemanticCheck {

  override def check(): Unit =
    labelConj match {
      case And(HasLabel(_), True()) =>
      case _ =>
        throw UnsupportedOperation("Label conjunction is not supported. An entity must have " +
          "only one label associated with it.")
    }
}
