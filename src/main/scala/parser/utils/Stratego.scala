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

package parser.utils

import org.spoofax.interpreter.terms.IStrategoTerm
import org.spoofax.terms.{StrategoAppl, StrategoConstructor, StrategoList, StrategoString}

/** Helper object for creating [[IStrategoTerm]] subtrees. */
object Stratego {
  def objectMatchPattern(labelPreds: IStrategoTerm, propsPreds: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "ObjectMatchPattern",
        /*arity = */ 2),
      /*kids = */ Array(labelPreds, propsPreds),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def objectConstructPattern(labelAssignments: IStrategoTerm,
                             propAssignments: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "ObjectConstructPattern",
        /*arity = */ 2),
      /*kids = */ Array(labelAssignments, propAssignments),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def none: IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "None",
        /*arity = */ 0),
      /*kids = */ Array.empty[IStrategoTerm],
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def some(child: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "Some",
        /*arity = */ 1),
      /*kids = */ Array(child),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def varDef(str: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "VarDef",
        /*arity = */ 1),
      /*kids = */ Array(str),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def varRefDef(str: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "VarRefDef",
        /*arity = */ 1),
      /*kids = */ Array(str),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def string(value: String): IStrategoTerm = {
    new StrategoString(
      /*value = */ value,
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def edge(edgePattern: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "Edge",
        /*arity = */ 1),
      /*kids = */ Array(edgePattern),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def edgeMatchPattern(varDef: IStrategoTerm, objMatchPattern: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "EdgeMatchPattern",
        /*arity = */ 2),
      /*kids = */ Array(varDef, objMatchPattern),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def edgeConstructPattern(varRefDef: IStrategoTerm,
                           copyPattern: IStrategoTerm,
                           groupDeclaration: IStrategoTerm,
                           objConstructPattern: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "EdgeConstructPattern",
        /*arity = */ 4),
      /*kids = */ Array(varRefDef, copyPattern, groupDeclaration, objConstructPattern),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def groupDeclaration(exprs: Seq[IStrategoTerm]): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ "GroupDeclaration",
        /*arity = */ 1),
      /*kids = */ Array(createStrategoList(exprs)),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  def conn(name: String, child: IStrategoTerm): IStrategoTerm = {
    new StrategoAppl(
      /*ctor = */ new StrategoConstructor(
        /*name = */ name,
        /*arity = */ 1),
      /*kids = */ Array(child),
      /*annotations = */ null,
      /*storageType = */ 0)
  }

  private def createStrategoList(elems: Seq[IStrategoTerm]): StrategoList = {
    elems match {
      case Seq(elem, other@_*) =>
        new StrategoList(
          /*head =*/ elem,
          /*tail =*/ createStrategoList(other),
          /*annotations =*/ null,
          /*storageType =*/ 0)
      case _ =>
        new StrategoList(
          /*head =*/ null,
          /*tail =*/ null,
          /*annotations =*/ null,
          /*storageType =*/ 0)
    }
  }
}
