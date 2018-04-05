package planner.target_api

import algebra.expressions.Reference

/** Stores metadata about the binding table created by the match clause. */
abstract class BindingTable {

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
