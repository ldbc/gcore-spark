package algebra.expressions

import algebra.exceptions.PropKeysException
import algebra.trees.{PropertyContext, SemanticCheckWithContext}
import common.compiler.Context
import schema.EntitySchema


case class PropertyKey(key: String) extends AlgebraExpression {
  children = List(Literal(key))

  def this(literal: Literal[String]) = this(literal.literalValue)
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
