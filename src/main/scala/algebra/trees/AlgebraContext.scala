package algebra.trees

import algebra.BindingTable
import schema.GraphDb

case class AlgebraContext(graphDb: GraphDb, bindingTable: BindingTable)
