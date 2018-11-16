/*
 * gcore-spark is the reference implementation of the G-CORE graph query
 * language by the Linked Data Benchmark Council (LDBC) - ldbcouncil.org
 *
 * The copyrights of the source code in this file belong to:
 * - CWI (www.cwi.nl), 2017-2018
 * - Universidad de Talca (2018)
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



import compiler.{CompileContext, Compiler, GcoreCompiler}
import org.apache.spark.sql.SparkSession
import schema.{Catalog, SchemaException}
import spark.{Directory, SparkCatalog}
import spark.examples.{CompanyGraph, DummyGraph, PeopleGraph, SocialGraph}
import org.apache.log4j.Logger
import org.apache.log4j.Level


/** Main entry point of the interpreter. */
object GcoreRunner {

  def newRunner: GcoreRunner = {
    val sparkSession: SparkSession = SparkSession
      .builder()
      .appName("G-CORE Runner")
      .master("local[*]")
      .getOrCreate()
    val catalog: SparkCatalog = SparkCatalog(sparkSession)
    val compiler: Compiler = GcoreCompiler(CompileContext(catalog, sparkSession))

    GcoreRunner(sparkSession, compiler, catalog)
  }

  def main(args: Array[String]): Unit = {

    var log = false
    activeLog(log)
    var active = true
    val gcoreRunner: GcoreRunner = GcoreRunner.newRunner

    if(args.length > 0 && args.head == "-d"){
      loadDatabase(gcoreRunner, args.last)
    }
    else{
      gcoreRunner.catalog.registerGraph(DummyGraph(gcoreRunner.sparkSession))
      gcoreRunner.catalog.registerGraph(PeopleGraph(gcoreRunner.sparkSession))
      gcoreRunner.catalog.registerGraph(SocialGraph(gcoreRunner.sparkSession))
      gcoreRunner.catalog.registerGraph(CompanyGraph(gcoreRunner.sparkSession))
      gcoreRunner.catalog.setDefaultGraph("social_graph")
      loadDatabase(gcoreRunner, "defaultDB")
    }


    options

    while(active)
    {
      var option = scala.io.StdIn.readLine("g-core=>: ")
      option.split(" ")(0) match {
        case "\\r" =>
          unregisterGraph(gcoreRunner, option)
        case "\\q" =>
          active = false
        case "\\c" =>
          setDefaultGraph(gcoreRunner, option)
        case "\\h" =>
          options
        case "\\l" =>
          println(gcoreRunner.catalog.toString)
        case "\\d" =>
          println(gcoreRunner.catalog.graph(option.split(" ")(1)).toString)
        case "\\v" =>
          log= !log
          activeLog(log)
        case _ =>
          if(option.length >0 && option.substring(option.length-1).trim == ";") {

            var query = option.replace(";", "").trim
            try {
            println(query)
            gcoreRunner.compiler.compile(
              query)
            }
            catch {
              case parseException: parser.exceptions.QueryParseException => println(" Query type unsupported for the moment")
              case defaultgNotAvalilable: algebra.exceptions.DefaultGraphNotAvailableException => println(" No default graph available")
              case analysisException: org.apache.spark.sql.AnalysisException => println("Error: " + analysisException.getMessage())
              case unsupportedOperation: common.exceptions.UnsupportedOperation => println("Error: " + unsupportedOperation.getMessage)
              case matchError: scala.MatchError => println("Error: " + matchError.getMessage())
              case disjunctLabels: algebra.exceptions.DisjunctLabelsException => println("Error: " + disjunctLabels.getMessage)
              case schemaExeption: SchemaException => println("Error: " + schemaExeption.getMessage)
              case _: Throwable => println("Unexpected exception")
            }
          }
          else
            println("Invalid Option")
      }
    }
  }

  def setDefaultGraph( gcoreRunner: GcoreRunner , graphp: String): String =
  {
    var r_graph = graphp.replace("\\c","").trim
    if (!gcoreRunner.catalog.hasGraph(r_graph))
    {
      println("Error: Graph not available")
    }
    else
    {
      gcoreRunner.catalog.setDefaultGraph(r_graph)
      println(r_graph +" "+ "set as default graph." )
    }
    graphp
  }

  def unregisterGraph(gcoreRunner: GcoreRunner , graphp: String) :Unit =
  {
    var r_graph = graphp.replace("\\r","").trim
    gcoreRunner.catalog.unregisterGraph(r_graph)
  }


  def options :Unit =
  {
    println(
      """
        |Options:
        |\h Help.
        |\c Set default graph.   (\c graph name)
        | ; Execute a query.     (ex. CONSTRUCT (x) MATCH (x);)
        |\l Graphs in database.
        |\d Graph information. (\d graph)
        |\v Log.
        |\q Quit.
      """.stripMargin)
  }

  def loadDatabase(gcoreRunner: GcoreRunner, folder:String):Unit=
  {
    var directory = new Directory
    var load = directory.loadDatabase(folder, gcoreRunner.sparkSession, gcoreRunner.catalog)
    gcoreRunner.catalog.databaseDirectory = folder

    if (load)
      println("Current Database: "+folder)
    else
      println("The database entered was not loaded correctly")
  }

  def activeLog(log: Boolean):Unit =
  {
    if(log)
    {
      Logger.getLogger("org").setLevel(Level.INFO)
      Logger.getLogger("akka").setLevel(Level.INFO)
      println("Log activated")
    }

    else
    {
      Logger.getLogger("org").setLevel(Level.OFF)
      Logger.getLogger("akka").setLevel(Level.OFF)
      println("Log deactivated")
    }

  }
}


case class GcoreRunner(sparkSession: SparkSession, compiler: Compiler, catalog: Catalog)
