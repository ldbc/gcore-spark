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

/**
  * A construct pattern of a variable used in the construct query. It is different from the
  * predicates used in the when sub-clause.
  *
  * For example, for the construct clause
  *
  * > CONSTRUCT (n:Person) WHEN n.strProp = "foo"
  *
  * n is the variable being built (a node) and its [[ObjectConstructPattern]] is the label
  * assignment "Person". The when sub-clause contains further filtering predicates for the matched
  * nodes, but the condition on the "strProp" attribute is not part of n's pattern.
  *
  * When constructing a new object, we can assign it new labels or use property values based on the
  * properties of matched patterns.
  */
case class ObjectConstructPattern(labelAssignments: LabelAssignments,
                                  propAssignments: PropAssignments) extends AlgebraExpression {
  children = List(labelAssignments, propAssignments)

  /**
    * Creates a new [[ObjectConstructPattern]], in which the [[labelAssignments]] are the set union
    * of this object's and other's [[labelAssignments]] and the [[propAssignments]] are the set
    * union of this object's and other's [[propAssignments]].
    */
  def merge(other: ObjectConstructPattern): ObjectConstructPattern =
    ObjectConstructPattern(
      labelAssignments = this.labelAssignments merge other.labelAssignments,
      propAssignments = this.propAssignments merge other.propAssignments)
}

object ObjectConstructPattern {
  val empty: ObjectConstructPattern =
    ObjectConstructPattern(
      labelAssignments = LabelAssignments(Seq.empty),
      propAssignments = PropAssignments(Seq.empty))
}

/** The aggregation start (*) symbol. */
case object Star extends AlgebraExpression
