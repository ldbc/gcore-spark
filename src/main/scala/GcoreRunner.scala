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



import java.io.File
import java.net.URI

import algebra.AlgebraRewriter
import compiler.{CompileContext, Compiler, GcoreCompiler}
import jline.UnsupportedTerminal
import jline.console.ConsoleReader
import jline.console.completer.FileNameCompleter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import schema.{Catalog, SchemaException}
import spark.{Directory, GraphSource, SparkCatalog}
import spark.examples.{BasicGraph, CompanyGraph, DummyGraph, PeopleGraph, SocialGraph}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import parser.SpoofaxParser



/** Main entry point of the interpreter. */
object GcoreRunner {

  import jline.TerminalFactory

  jline.TerminalFactory.registerFlavor(TerminalFactory.Flavor.WINDOWS, classOf[UnsupportedTerminal])
  def newRunner: GcoreRunner = {
    val sparkSession: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("G-CORE Runner")
      .getOrCreate()
    val catalog: SparkCatalog = SparkCatalog(sparkSession)
    val compiler: Compiler = GcoreCompiler(CompileContext(catalog, sparkSession))
    println(sparkSession.version)
    GcoreRunner(sparkSession, compiler, catalog)

  }

  def main(args: Array[String]): Unit = {

    var log = false
    activeLog(log)
    var active = true
    val gcoreRunner: GcoreRunner = GcoreRunner.newRunner

    if(args.size > 0 && args.head == "-d"){
      loadDatabase(gcoreRunner, args.last)
    }
    else{
      /*gcoreRunner.catalog.registerGraph(DummyGraph(gcoreRunner.sparkSession))
      gcoreRunner.catalog.registerGraph(PeopleGraph(gcoreRunner.sparkSession))
      gcoreRunner.catalog.registerGraph(SocialGraph(gcoreRunner.sparkSession))
      gcoreRunner.catalog.registerGraph(CompanyGraph(gcoreRunner.sparkSession))*/
      gcoreRunner.catalog.registerGraph(BasicGraph(gcoreRunner.sparkSession))

      gcoreRunner.catalog.setDefaultGraph("basic_graph")
      loadDatabase(gcoreRunner, "defaultDB")
    }


    options
    val console = new ConsoleReader()
    console.addCompleter(new FileNameCompleter())
    console.setPrompt("g-core=>")
    var line = ""

    while (active){
      //try {
      line = console.readLine()
      line.split(" ")(0) match {
        case "\\q" =>
          active = false
        case "\\c" =>
          setDefaultGraph(gcoreRunner, line)
        case "\\g" =>
          loadGraphFromJson(gcoreRunner, line.replace("\\g","").trim)
        case "\\h" =>
          options
        case "\\l" =>
          println(gcoreRunner.catalog.toString)
        case "\\d" =>
          println(gcoreRunner.catalog.graph(line.split(" ")(1)).toString)
        case "\\v" =>
          log = !log
          activeLog(log)
        case _ =>
          if (line.length > 0 && line.substring(line.length - 1).trim == ";") {

            var query = line.replace(";", "").trim

            println(query)
            gcoreRunner.compiler.compile(
              query)


          }
          else
            println("Invalid Option")
      }
      //}
      /*catch {
         case parseException: parser.exceptions.QueryParseException => println(parseException.getMessage)
         case defaultgNotAvalilable: algebra.exceptions.DefaultGraphNotAvailableException => println(" No default graph available")
         case analysisException: org.apache.spark.sql.AnalysisException => println("Error: " + analysisException.getMessage())
         case unsupportedOperation: common.exceptions.UnsupportedOperation => println("Error: " + unsupportedOperation.getMessage)
         case matchError: scala.MatchError => println("Error: " + matchError.getMessage())
         case disjunctLabels: algebra.exceptions.DisjunctLabelsException => println("Error: " + disjunctLabels.getMessage)
         case schemaExeption: SchemaException => println("Error: " + schemaExeption.getMessage)
         case indexOutOfBoundsException: IndexOutOfBoundsException => println("Error: Missing parameters")
         case _: Throwable => println("Unexpected exception")
       }*/
    }

  }

  def loadGraphFromJson(gcoreRunner: GcoreRunner, jsonName: String):Unit = {

    val hdfs = FileSystem.get(new URI("hdfs://localhost:9000/"), new Configuration())
    val path = new Path(jsonName)
    val jsonFile = hdfs.open(path)
    //val jsonFile: File = new File(jsonName)
    if(jsonFile != null){
      val graphSource = new GraphSource(gcoreRunner.sparkSession) {
        override val loadDataFn: String => DataFrame = _ => gcoreRunner.sparkSession.emptyDataFrame
      }

      //TODO Aldana show a message if it cannot register the graph
      gcoreRunner.catalog.asInstanceOf[SparkCatalog].registerGraph(graphSource, jsonFile)
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
    return graphp
  }

  def unregisterGraph(gcoreRunner: GcoreRunner , graphp: String) :Unit =
  {
    var r_graph = graphp.replace("\\r","").trim
    gcoreRunner.catalog.unregisterGraph(r_graph)
  }


  def options: Unit =
  {
    println(
      """
        |Options:
        |\h Help.
        |\c Set default graph.   (\c graph name)
        | ; Execute a query.     (ex. CONSTRUCT (x) MATCH (x);)
        |\l Graphs in database.
        |\d Graph information. (\d graph name)
        |\v Log.
        |\g Load graph from file.   (\g file_name.json)
        |\q Quit.
      """.stripMargin);
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

  def activeLog(log: Boolean) =
  {
    if(log)
    {
      Logger.getLogger("org").setLevel(Level.INFO)
      Logger.getLogger("akka").setLevel(Level.INFO)
      Logger.getRootLogger.setLevel(Level.INFO)
      Logger.getLogger("algebra.AlgebraRewriter").setLevel(Level.INFO)
      Logger.getLogger("parser.SpoofaxParser").setLevel(Level.INFO)
      Logger.getLogger("spark.sql.SqlPlanner").setLevel(Level.INFO)
      Logger.getLogger("spark.sql.operators.PathSearch").setLevel(Level.INFO)
      println("Log activated")
    }

    else
    {
      Logger.getLogger("org").setLevel(Level.ERROR )
      Logger.getLogger("akka").setLevel(Level.ERROR )
      Logger.getRootLogger.setLevel(Level.ERROR )
      Logger.getLogger(SpoofaxParser.getClass.getName).setLevel(Level.ERROR )
      Logger.getLogger("algebra.AlgebraRewriter").setLevel(Level.ERROR )
      Logger.getLogger("parser.SpoofaxParser").setLevel(Level.ERROR )
      Logger.getLogger("spark.sql.SqlPlanner").setLevel(Level.ERROR )
      Logger.getLogger("spark.sql.operators.PathSearch").setLevel(Level.ERROR )
      println("Log deactivated")
    }

  }
}


case class GcoreRunner(sparkSession: SparkSession, compiler: Compiler, catalog: Catalog)