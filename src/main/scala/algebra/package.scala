import algebra.expressions.Reference
import algebra.operators.RelationLike

import scala.collection.mutable

package object algebra {

  type BindingTable =
    mutable.HashMap[Reference, mutable.Set[RelationLike]]
      with mutable.MultiMap[Reference, RelationLike]

  def newBindingTable: BindingTable = new mutable.HashMap[Reference, mutable.Set[RelationLike]]
    with mutable.MultiMap[Reference, RelationLike]
}
