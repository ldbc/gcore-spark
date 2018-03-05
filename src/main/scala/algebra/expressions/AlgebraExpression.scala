package algebra.expressions

import common.compiler.Context
import algebra.operators.Query
import algebra.exceptions.{DisjunctLabelsException, PropKeysException, SemanticException, UnsupportedOperation}
import algebra.trees._
import schema.EntitySchema

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

  def this(literal: Literal[String]) = this(literal.literalValue)

  override def toString: String = s"$name [$value]"
}

case class PropertyKey(key: String) extends AlgebraExpression {
  children = List(Literal(key))

  def this(literal: Literal[String]) = this(literal.literalValue)

  override def toString: String = s"$name [$key]"
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

/** A predicate that asserts that the graph entity satisfies all the given property conditions. */
case class WithProps(propConj: AlgebraExpression) extends PredicateExpression(propConj)
  with SemanticCheckWithContext {

  children = List(propConj)

  override def checkWithContext(context: Context): Unit = {
    val withLabels = context.asInstanceOf[PropertyContext].labelsExpr
    val schema: EntitySchema = context.asInstanceOf[PropertyContext].schema

    val propKeys: Seq[PropertyKey] = {
      val pks = new collection.mutable.ArrayBuffer[PropertyKey]()
      propConj.forEachDown {
        case pk @ PropertyKey(_) => pks += pk
        case _ =>
      }
      pks
    }

    val expectedProps: Seq[PropertyKey] = {
      if (withLabels.isDefined) {
        val labels: Seq[Label] = {
          val ls = new collection.mutable.ArrayBuffer[Label]()
          withLabels.get.forEachDown {
            case l@Label(_) => ls += l
            case _ =>
          }
          ls
        }
        labels.flatMap(label => schema.properties(label))
      } else
        schema.properties
    }

    val unavailablePropKeys: Seq[PropertyKey] = propKeys filterNot expectedProps.contains
    if (unavailablePropKeys.nonEmpty)
      throw
        PropKeysException(
          graphName = context.asInstanceOf[PropertyContext].graphName,
          unavailableProps = unavailablePropKeys,
          schema = schema)
  }
}

// TODO: Add built-in and aggregate functions.
