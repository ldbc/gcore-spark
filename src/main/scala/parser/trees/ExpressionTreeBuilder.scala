package parser.trees

import algebra.expressions._
import parser.exceptions.QueryParseException
import parser.trees.MatchTreeBuilder.extractGraphPattern

object ExpressionTreeBuilder {

  /** Builds an [[AlgebraExpression]] from any possible lexical node of an expression. */
  def extractExpression(from: SpoofaxBaseTreeNode): AlgebraExpression = {
    from.name match {
      /* Unary expressions. */
      case "Not" => Not(extractExpression(from.children.head))
      case "UMin" => Minus(extractExpression(from.children.head))

      /* Logical expressions. */
      case "And" =>
        And(extractExpression(from.children.head), extractExpression(from.children.last))
      case "Or" => Or(extractExpression(from.children.head), extractExpression(from.children.last))

      /* Conditional expressions. */
      case "Eq" => Eq(extractExpression(from.children.head), extractExpression(from.children.last))
      case "Neq1" =>
        Neq(extractExpression(from.children.head), extractExpression(from.children.last))
      case "Neq2" =>
        Neq(extractExpression(from.children.head), extractExpression(from.children.last))
      case "Gt" => Gt(extractExpression(from.children.head), extractExpression(from.children.last))
      case "Gte" =>
        Gte(extractExpression(from.children.head), extractExpression(from.children.last))
      case "Lt" => Lt(extractExpression(from.children.head), extractExpression(from.children.last))
      case "Lte" =>
        Lte(extractExpression(from.children.head), extractExpression(from.children.last))

      /* Math expressions. */
      case "Pow" =>
        Power(extractExpression(from.children.head), extractExpression(from.children.last))

      /* Arithmetic expressions. */
      case "Mul" =>
        Mul(extractExpression(from.children.head), extractExpression(from.children.last))
      case "Div" =>
        Div(extractExpression(from.children.head), extractExpression(from.children.last))
      case "Mod" =>
        Mod(extractExpression(from.children.head), extractExpression(from.children.last))
      case "Add" =>
        Add(extractExpression(from.children.head), extractExpression(from.children.last))
      case "Sub" =>
        Sub(extractExpression(from.children.head), extractExpression(from.children.last))

      /* Predicate expressions. */
      case "IsNull" => IsNull(extractExpression(from.children.head))
      case "IsNotNull" => IsNotNull(extractExpression(from.children.head))

      /* TODO: List expressions. */

      /* TODO: Aggregate expressions. */

      /* TODO: Function calls. */

      /* Other algebra expressions. */
      case "VarRef" => Reference(from.children.head.asInstanceOf[SpoofaxLeaf[String]].value)
      case "PropRef" =>
        PropertyRef(
          ref = extractExpression(from.children.head).asInstanceOf[Reference],
          propKey = PropertyKey(from.children(1).asInstanceOf[SpoofaxLeaf[String]].value))
      case "Prop" =>
        PropAssignment(
          propKey = PropertyKey(from.children.head.asInstanceOf[SpoofaxLeaf[String]].value),
          expr = extractExpression(from.children.last))
      case "Label" =>
        Label(from.children.head.children.head.asInstanceOf[SpoofaxLeaf[String]].value)
      case "Integer" => IntLiteral(from.children.head.asInstanceOf[SpoofaxLeaf[String]].value.toInt)
      case "True" => True
      case "False" => False
      case "String" => StringLiteral(from.children.head.asInstanceOf[SpoofaxLeaf[String]].value)
      case "BasicGraphPattern" => Exists(extractGraphPattern(from))

      /* Default case. */
      case _ => throw QueryParseException(s"Unsupported expression ${from.name}")
    }
  }
}
