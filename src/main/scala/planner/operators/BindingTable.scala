package planner.operators

import algebra.expressions.Reference

abstract class BindingTable {
  type SchemaType
  type QueryOperand

  val schemaMap: Map[Reference, SchemaType]
  val btableSchema: SchemaType
  val btable: QueryOperand
}
