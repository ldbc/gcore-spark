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

package algebra.operators

/** A column in a relation. */
case class Column(columnName: String) extends RelationalOperator {

  override def name: String = s"${super.name} [$columnName]"
}

object Column {

  /**
    * Fixed column names we expect to find in physical tables, for specific entity types:
    * - Vertex, edge and path tables should contain the [[ID_COL]];
    * - Edge and path tables should contain the [[FROM_ID_COL]] and [[TO_ID_COL]];
    * - Path tables should contain the [[EDGE_SEQ_COL]] and [[COST_COL]].
    */
  val ID_COL: Column = Column("id")
  val FROM_ID_COL: Column = Column("fromId")
  val TO_ID_COL: Column = Column("toId")
  val EDGE_SEQ_COL: Column = Column("edges")
  val COST_COL: Column = Column("cost")

  /** Other fixed column names as needed by targets. */
  val TABLE_LABEL_COL: Column = Column("table_label")
  val CONSTRUCT_ID_COL: Column = Column("construct_id")
}
