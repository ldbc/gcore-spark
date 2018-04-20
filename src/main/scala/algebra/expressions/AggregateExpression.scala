package algebra.expressions

abstract class AggregateExpression(distinct: Boolean, expr: AlgebraExpression, symbol: String)
  extends UnaryExpression(expr, symbol) {

  override def name: String = s"${super.name} [distinct = $distinct]"

  def isDistinct: Boolean = distinct
}

case class Collect(distinct: Boolean, expr: AlgebraExpression)
  extends AggregateExpression(distinct, expr, "COLLECT_LIST")

case class Count(distinct: Boolean, expr: AlgebraExpression)
  extends AggregateExpression(distinct, expr, "COUNT")

case class Min(distinct: Boolean, expr: AlgebraExpression)
  extends AggregateExpression(distinct, expr, "MIN")

case class Max(distinct: Boolean, expr: AlgebraExpression)
  extends AggregateExpression(distinct, expr, "MAX")

case class Sum(distinct: Boolean, expr: AlgebraExpression)
  extends AggregateExpression(distinct, expr, "SUM")

case class Avg(distinct: Boolean, expr: AlgebraExpression)
  extends AggregateExpression(distinct, expr, "AVG")

case class GroupConcat(distinct: Boolean, expr: AlgebraExpression)
  extends AggregateExpression(distinct, expr, "GROUP_CONCAT")
