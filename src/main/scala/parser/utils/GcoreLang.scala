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

import java.net.URL

import org.apache.commons.vfs2.FileObject
import org.metaborg.core.language.{ILanguageComponent, ILanguageDiscoveryRequest, ILanguageImpl, LanguageUtils}
import org.metaborg.spoofax.core.Spoofax
import org.metaborg.spoofax.core.unit.{ISpoofaxInputUnit, ISpoofaxParseUnit}
import org.spoofax.interpreter.terms.IStrategoTerm._
import org.spoofax.interpreter.terms.{IStrategoAppl, IStrategoInt, IStrategoString, IStrategoTerm}
import parser.exceptions.{LanguageLoadException, QueryParseException}

/** Utility object to work with the [[Spoofax]] parser and the G-CORE language component. */
object GcoreLang {

  /** File name of the G-CORE language component. */
  val GCORE_GRAMMAR_SPEC: String = "gcore-spoofax-0.1.0-SNAPSHOT.spoofax-language"

  val spoofax: Spoofax = new Spoofax()

  /**
    * The G-CORE language component we can use once [[Spoofax]] has loaded the grammar specification
    * from [[GCORE_GRAMMAR_SPEC]].
    */
  val gcoreLang: ILanguageImpl = {
    val gcoreUrl: URL = getClass.getClassLoader.getResource(GCORE_GRAMMAR_SPEC)
    val gcoreLocation: FileObject = spoofax.resourceService.resolve("zip:" + gcoreUrl + "!/")
    val requests: java.lang.Iterable[ILanguageDiscoveryRequest] =
      spoofax.languageDiscoveryService.request(gcoreLocation)
    val components: java.lang.Iterable[ILanguageComponent] =
      spoofax.languageDiscoveryService.discover(requests)
    val implementations: java.util.Set[ILanguageImpl] = LanguageUtils.toImpls(components)
    val lang = LanguageUtils.active(implementations)
    if (lang == null)
      throw LanguageLoadException(s"No language implementation was found at $GCORE_GRAMMAR_SPEC")
    lang
  }

  /** Parses a G-CORE query. */
  def parseQuery(query: String): IStrategoTerm = {
    val input: ISpoofaxInputUnit = spoofax.unitService.inputUnit(query, gcoreLang, null)
    val output: ISpoofaxParseUnit = spoofax.syntaxService.parse(input)
    if (!output.valid())
      throw QueryParseException(s"Could not parse query $query")
    output.ast()
  }

  def prettyPrint(term: IStrategoTerm): Unit = {
    def withIndent(term: IStrategoTerm, level: Int): Unit = {
      term.getTermType match {
        case APPL =>
          println(s"${" " * level}${term.asInstanceOf[IStrategoAppl].getConstructor.getName}")
          term.getAllSubterms.foreach(subTerm => withIndent(subTerm, level + 2))
        case STRING =>
          println(s"${" " * level}${term.asInstanceOf[IStrategoString].stringValue()}")
        case INT =>
          println(s"${" " * level}${term.asInstanceOf[IStrategoInt].intValue()}")
        case IStrategoTerm.LIST =>
          var name: String = if (term.getSubtermCount == 0) "EmptyList" else "List"
          println(s"${" " * level}$name")
          term.getAllSubterms.foreach(subTerm => withIndent(subTerm, level + 2))
        case _ =>
      }
    }

    withIndent(term, 0)
  }
}
