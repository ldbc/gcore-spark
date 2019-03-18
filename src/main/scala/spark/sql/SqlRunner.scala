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

package spark.sql

import algebra.operators._
import algebra.trees.AlgebraTreeNode
import common.exceptions.UnsupportedOperation
import compiler.{CompileContext, RunTargetCodeStage}
import org.apache.spark.sql.DataFrame
import schema.PathPropertyGraph
import spark.{Directory, SaveGraph}

/** Runs the query plan created by the [[SqlPlanner]] on Spark. */
case class SqlRunner(compileContext: CompileContext) extends RunTargetCodeStage {

  override def runStage(input: AlgebraTreeNode): PathPropertyGraph = {
    val sparkSqlPlanner: SqlPlanner = SqlPlanner(compileContext)
    input match {
      case buildGraph: GraphBuild =>
        val matchClause: AlgebraTreeNode = buildGraph.matchClause
        val groupConstructs: Seq[AlgebraTreeNode] = buildGraph.groupConstructs
        val matchWhere: AlgebraTreeNode = buildGraph.matchWhere

        val matchData: DataFrame = sparkSqlPlanner.solveBindingTable(matchClause,matchWhere)
        val constructBindingTable : DataFrame = sparkSqlPlanner.generateConstructBindingTable(matchData, groupConstructs)
        val graph: PathPropertyGraph = sparkSqlPlanner.constructGraph(constructBindingTable, groupConstructs)
        graph


      case storeGraph : Create =>
        var graph: PathPropertyGraph = runStage(storeGraph.children.head)
        graph.graphName = storeGraph.getGraphName
        compileContext.catalog.registerGraph(graph)

        val saveGraph = SaveGraph()
        saveGraph.saveJsonGraph(graph,compileContext.catalog.databaseDirectory)
        println("The graph was created")
        graph

      case dropGraph : Drop =>
        var graph = compileContext.catalog.graph(dropGraph.graphName)
        compileContext.catalog.unregisterGraph(graph)
        var directory: Directory = new Directory
        var dropped = directory.deleteGraph(graph.graphName,compileContext.catalog.databaseDirectory)
        if(dropped)
          println("The graph was successfully dropped")
        else
          println("The graph was only dropped from the catalog, please check database directory.")
        null

      case viewGraph : View =>
        var graph: PathPropertyGraph = runStage(viewGraph.children.head)
        graph.graphName = viewGraph.getGraphName
        compileContext.catalog.registerGraph(graph)
        println("The graph view was created")
        graph

      case _ =>
        throw UnsupportedOperation(s"Cannot run query on input type ${input.name}")
    }
  }
}
