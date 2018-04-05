package algebra.expressions

import algebra.trees._
import algebra.types.GraphPattern

/**
  * A G-CORE expressions as defined at
  * https://github.com/ldbc/ldbc_gcore_parser/blob/master/gcore-spoofax/syntax/Expressions.sdf3.
  */
abstract class AlgebraExpression extends AlgebraTreeNode

/** The name of a variable (binding). */
case class Reference(refName: String) extends AlgebraExpression {
  override def toString: String = s"$name [$refName]"
}

/**
  * An existential sub-clause in a match predicate.
  *
  * For example, for the match clause
  *
  * > MATCH (n) WHERE (n:Person)
  *
  * the existential sub-query is represented by the (n:Person) pattern and its meaning is that it
  * restricts matched nodes (n) to those that are labeled as persons.
  */
case class Exists(graphPattern: GraphPattern) extends AlgebraExpression {
  children = List(graphPattern)
}

/**
  * A matching pattern of a variable used in the match query. It is different from the predicates
  * used in the where sub-clause.
  *
  * For example, for the match clause
  *
  * > MATCH (n:Person) WHERE n.strProp = "foo"
  *
  * n is the variable being matched (a node) and its [[ObjectPattern]] is the label "Person". The
  * where sub-clause contains further filtering predicates for the matched nodes, but the condition
  * on the "strProp" attribute is not part of n's pattern.
  */
case class ObjectPattern(labelsPred: AlgebraExpression, propsPred: AlgebraExpression)
  extends AlgebraExpression {
  children = List(labelsPred, propsPred)
}
