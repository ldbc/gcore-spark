/*
 * gcore-spark is the reference implementation of the G-CORE graph query
 * language by the Linked Data Benchmark Council (LDBC) - ldbcouncil.org
 *
 * The copyrights of the source code in this file belong to:
 * - CWI (www.cwi.nl), 2017-2018
 * - Universidad de Talca (www.utalca.cl), 2018
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

package algebra.types

/** A G-CORE data type. */
abstract class GcoreDataType extends AlgebraType {

  /** The equivalent type in Scala of this [[GcoreDataType]]. */
  type ScalaType
}

/**
  * Available data types in G-CORE queries, as defined at
  * https://github.com/ldbc/ldbc_gcore_parser/blob/master/gcore-spoofax/syntax/Literals.sdf3
  */
case object GcoreInteger extends GcoreDataType { override type ScalaType = Long }
case object GcoreDecimal extends GcoreDataType { override type ScalaType = Double }
case object GcoreString extends GcoreDataType { override type ScalaType = String }
case object GcoreBoolean extends GcoreDataType { override type ScalaType = Boolean }
case object GcoreArray extends GcoreDataType { override type ScalaType = Int }
case object GcoreDate extends GcoreDataType { override type ScalaType = String }
case object GcoreTimestamp extends GcoreDataType { override type ScalaType = String }
case object GcoreNull extends GcoreDataType { override type ScalaType = Object }
