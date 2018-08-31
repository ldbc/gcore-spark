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

package algebra.target_api

import algebra.expressions.Reference

/** Stores metadata about the binding table created by the match clause. */
abstract class BindingTableMetadata {

  /** The target-specific data type of the schema. */
  type SchemaType

  /**
    * The target-specific data type of the query operand. For example for a SQL query it can be a
    * String.
    */
  type QueryOperand

  /**
    * A mapping from a match variable to its schema.
    *
    * Conceptually, the binding table is a relation where each property is a variable in the match
    * clause. If the match happened betwen three variables, u, v and w, the binding table could be
    * viewed as the table:
    *
    * +---+---+---+
    * | u | v | w |
    * +---+---+---+
    * |.. |.. |.. |
    *
    * However, at target level, each entry in the table is a complex value, being a relation in
    * itself. The real physical tables could be viewed as:
    *
    * +------+---------+---       +------+---------+---       +------+---------+---
    * | u.id | u.prop1 | ...      | v.id | v.prop1 | ...      | w.id | w.prop1 | ..
    * +------+---------+---       +------+---------+---       +------+---------+---
    * |  ... |   ...   | ..       |  ... |   ...   | ..       |  ... |   ...   | ..
    *
    * This mapping stores the schema of each variable's relation.
    */
  val schemaMap: Map[Reference, SchemaType]

  /**
    * The schema of the binding table.
    *
    * After solving the relational operations on the algebraic tree, the binding table may contain
    * data very different from its conceptual schema in which each variable is a column or from its
    * variable's relations.
    */
  val btableSchema: SchemaType

  /**
    * The binding table at a given point in the query evaluation, expressed as a [[QueryOperand]].
    * By expressing it this way, we allow nodes in the relational tree to operate directly on tables
    * produced by their children nodes.
    */
  val btable: QueryOperand
}
