package ir.algebra.expressions

import ir.algebra.Query
import ir.trees.AlgebraTreeNode

/**
  * The G-CORE expressions defined at
  * https://github.com/ldbc/ldbc_gcore_parser/blob/master/gcore-spoofax/syntax/Expressions.sdf3.
  */

abstract class AlgebraExpression extends AlgebraTreeNode
case class True() extends AlgebraExpression
case class False() extends AlgebraExpression

/** An entity (vertex, edge, path) name. */
case class Reference(refName: String) extends AlgebraExpression {
  override def toString: String = s"$name [$refName]"
}

/** An actual value, such as strings, integers, booleans True and False, dates, etc. */
case class Literal[T](literalValue: T) extends AlgebraExpression {
  override def toString: String = s"$name [$literalValue]"
}

case class Label(value: String) extends AlgebraExpression {
  children = List(Literal(value))

  override def toString: String = s"$name [$value]"
}

case class PropertyKey(key: String) extends AlgebraExpression {
  children = List(Literal(key))

  override def toString: String = s"$name [$key]"
}

/**
  * A predicate that asserts that the graph entity has at least one of the given labels. It is a
  * disjunction of labels. The labels are expressed through their [[Literal]] value.
  */
case class HasLabel(labels: Seq[Label]) extends AlgebraExpression {
  children = labels
}

case class Exists(query: Query) extends AlgebraExpression {
  children = List(query)
}


/** Unary expressions. */
abstract class UnaryExpression(expr: AlgebraExpression) extends AlgebraExpression {
  children = List(expr)
}

case class Minus(expr: AlgebraExpression) extends UnaryExpression(expr)
case class Not(expr: AlgebraExpression) extends UnaryExpression(expr)


/** Binary expressions. */
abstract class BinaryExpression(lhs: AlgebraExpression, rhs: AlgebraExpression)
  extends AlgebraExpression {

  children = List(lhs, rhs)
}

case class Power(lhs: AlgebraExpression, rhs: AlgebraExpression) extends BinaryExpression(lhs, rhs)
case class Mul(lhs: AlgebraExpression, rhs: AlgebraExpression) extends BinaryExpression(lhs, rhs)
case class Div(lhs: AlgebraExpression, rhs: AlgebraExpression) extends BinaryExpression(lhs, rhs)
case class Mod(lhs: AlgebraExpression, rhs: AlgebraExpression) extends BinaryExpression(lhs, rhs)
case class Add(lhs: AlgebraExpression, rhs: AlgebraExpression) extends BinaryExpression(lhs, rhs)
case class Sub(lhs: AlgebraExpression, rhs: AlgebraExpression) extends BinaryExpression(lhs, rhs)

case class Eq(lhs: AlgebraExpression, rhs: AlgebraExpression) extends BinaryExpression(lhs, rhs)
case class Gt(lhs: AlgebraExpression, rhs: AlgebraExpression) extends BinaryExpression(lhs, rhs)
case class Lt(lhs: AlgebraExpression, rhs: AlgebraExpression) extends BinaryExpression(lhs, rhs)
case class Gte(lhs: AlgebraExpression, rhs: AlgebraExpression) extends BinaryExpression(lhs, rhs)
case class Lte(lhs: AlgebraExpression, rhs: AlgebraExpression) extends BinaryExpression(lhs, rhs)
case class Neq(lhs: AlgebraExpression, rhs: AlgebraExpression) extends BinaryExpression(lhs, rhs)
case class In(lhs: AlgebraExpression, rhs: AlgebraExpression) extends BinaryExpression(lhs, rhs)

case class And(lhs: AlgebraExpression, rhs: AlgebraExpression) extends BinaryExpression(lhs, rhs)
case class Or(lhs: AlgebraExpression, rhs: AlgebraExpression) extends BinaryExpression(lhs, rhs)


/** Predicate expressions. */
abstract class PredicateExpression(expr: AlgebraExpression) extends UnaryExpression(expr)
case class IsNull(expr: AlgebraExpression) extends PredicateExpression(expr)
case class IsNotNull(expr: AlgebraExpression) extends PredicateExpression(expr)

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
case class WithLabels(labels: AlgebraExpression) extends PredicateExpression(labels)

/** A predicate that asserts that the graph entity satisfies all the given property conditions. */
case class WithProps(props: AlgebraExpression) extends PredicateExpression(props)

// TODO: Add built-in and aggregate functions.
