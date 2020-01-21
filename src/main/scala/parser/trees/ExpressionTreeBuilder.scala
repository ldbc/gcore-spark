/*
 * gcore-spark is the reference implementation of the G-CORE graph query
 * language by the Linked Data Benchmark Council (LDBC) - ldbcouncil.org
 *
 * The copyrights of the source code in this file belong to:
 * - CWI (www.cwi.nl), 2017-2018
 *
 * This software is released in open source under the Apache License, 
 * Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

      /* Aggregate expressions. */
      case "Star" => Star
      case "collect" =>
        Collect(
          distinct = from.children.head.name == "Some",
          expr = extractExpression(from.children.last))
      case "count" =>
        Count(
          distinct = from.children.head.name == "Some",
          expr = extractExpression(from.children.last))
      case "min" =>
        Min(
          distinct = from.children.head.name == "Some",
          expr = extractExpression(from.children.last))
      case "max" =>
        Max(
          distinct = from.children.head.name == "Some",
          expr = extractExpression(from.children.last))
      case "sum" =>
        Sum(
          distinct = from.children.head.name == "Some",
          expr = extractExpression(from.children.last))
      case "avg" =>
        Avg(
          distinct = from.children.head.name == "Some",
          expr = extractExpression(from.children.last))
      case "group-concat" =>
        GroupConcat(
          distinct = from.children.head.name == "Some",
          expr = extractExpression(from.children.last))

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
      case "Null" => Null
      case "Date" => DateLiteral(from.children.head.asInstanceOf[SpoofaxLeaf[String]].value)
      case "Timestamp" => TimeStampLiteral(from.children.head.asInstanceOf[SpoofaxLeaf[String]].value)

      /* Default case. */
      case _ => throw QueryParseException(s"Unsupported expression ${from.name}")
    }
  }
}
