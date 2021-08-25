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

import common.trees.TreeNode
import org.spoofax.interpreter.terms.IStrategoTerm.{APPL, INT, LIST, STRING}
import org.spoofax.interpreter.terms.{IStrategoAppl, IStrategoInt, IStrategoString, IStrategoTerm}

import scala.reflect.ClassTag

object SpoofaxBaseTreeNode {

  /**
    * Handy extractor for easier pattern matching [[SpoofaxBaseTreeNode]]s. This will skip the
    * Stratego terms and compare directly on name.
    */
  def unapply(arg: SpoofaxBaseTreeNode): Option[String] = Some(arg.name)
}

/** A node in a Spoofax parse tree. */
abstract class SpoofaxBaseTreeNode(term: IStrategoTerm) extends TreeNode[SpoofaxBaseTreeNode] {

  // This assumes we have no nested lists, like for eg:
  // term children = [APPL, [APPL, LIST], APPL]
  // After unwrapping the list above, we get: [APPL, APPL, LIST, APPL], so the second list remains
  // unwrapped.
  // TODO: Do we have any cases of nested lists?
  children = {
    term
      .getAllSubterms
      .toList
      .flatMap(subTerm => {
        subTerm.getTermType match {
          case LIST => subTerm.getAllSubterms.toList
          case _ => List(subTerm)
        }
      })
      .map(term => term.getTermType match {
        case APPL => SpoofaxTreeNode(term)
        case INT => SpoofaxLeaf[Integer](term, term.asInstanceOf[IStrategoInt].intValue())
        case STRING => SpoofaxLeaf[String](term, term.asInstanceOf[IStrategoString].stringValue())
      })
  }
}

case class SpoofaxTreeNode(term: IStrategoTerm) extends SpoofaxBaseTreeNode(term) {
  override def name: String = term.asInstanceOf[IStrategoAppl].getConstructor.getName
}

case class SpoofaxLeaf[ValueType](term: IStrategoTerm, leafValue: ValueType)
                                 (implicit tag: ClassTag[ValueType])
  extends SpoofaxBaseTreeNode(term) {

  children = List.empty

  def value: ValueType = leafValue

  override def toString: String = s"$name [$value]"
}
