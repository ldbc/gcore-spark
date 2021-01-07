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
import java.nio.file.Paths

import compiler.{CompileContext, Compiler, GcoreCompiler}
import jline.UnsupportedTerminal
import jline.console.ConsoleReader
import jline.console.completer.FileNameCompleter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, SparkSession}
import schema.{Catalog, SchemaException}
import spark.{Directory, GraphSource, SaveGraph, SparkCatalog}
import spark.examples.BasicGraph
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
    GcoreRunner(sparkSession, compiler, catalog)
  }

  def main(args: Array[String]): Unit = {

    var log = false

    activeLog(log)
    var active = true
    val gcoreRunner: GcoreRunner = GcoreRunner.newRunner
    val fileSeparator = File.separator
    var hdfsUri= "";
    var pathDir= fileSeparator+"g-core"+fileSeparator;
    var database= "default-db";
    val usageMessage = "Usage: -h <HDFS URI> -p <DIRECTORY PATH> -d <DATABASE NAME>(Optional)"

    /*if(args.isEmpty){
      println(usageMessage)
      return
    }*/


    val argList= args.toSeq
    for (i <- 0 to argList.size-1){
      if(argList(i).equals("-h")) {
        hdfsUri=argList(i+1).trim
        if(!hdfsUri.endsWith(fileSeparator))
          hdfsUri= hdfsUri+fileSeparator
      } else if(argList(i).equals("-p")) {
        pathDir= argList(i+1).trim
        if(!pathDir.endsWith(fileSeparator))
          pathDir= pathDir+fileSeparator
        if (pathDir.startsWith(fileSeparator))
          pathDir=pathDir.substring(1)
      } else if(argList(i).equals("-d"))
        database=argList(i+1).trim

    }
    var dbUri = ""

    if(hdfsUri.isEmpty){
      val jarFile = new File(this.getClass.getProtectionDomain.getCodeSource.getLocation.toURI.getPath)
      hdfsUri= jarFile.getParentFile.getPath
      dbUri = Paths.get(hdfsUri, pathDir, database).toString
    }
    else{
      dbUri = hdfsUri + pathDir + database
      if(dbUri.contains(fileSeparator+fileSeparator))
        dbUri=dbUri.replace("\\",fileSeparator)

    }
    println(dbUri)
    loadDatabase(gcoreRunner, dbUri,hdfsUri)


    options
    val console = new ConsoleReader()
    console.addCompleter(new FileNameCompleter())
    console.setPrompt("g-core=>")
    var line = ""

    while (active){
      try {
        line = console.readLine()
        line.split(" ")(0) match {
          case "\\b" =>
            registerBasicGraph(gcoreRunner)
          case "\\q" =>
            active = false
          case "\\c" =>
            setDefaultGraph(gcoreRunner, line)
          case "\\i" =>
            val dir = gcoreRunner.catalog.databaseDirectory+"/free_graph"
            loadGraphFromJson(gcoreRunner, line.replace("\\i","").trim)
          case "\\h" =>
            options
          case "\\l" =>
            println(gcoreRunner.catalog.toString)
          case "\\s" =>
            println(gcoreRunner.catalog.graph(line.split(" ")(1)).toString)
          case "\\r" =>
            removeGraphFreeGraphDataframe(gcoreRunner, line.replace("\\r","").trim)
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
      }
      catch {
         case parseException: parser.exceptions.QueryParseException => println(parseException.getMessage)
         case defaultgNotAvalilable: algebra.exceptions.DefaultGraphNotAvailableException => println(" No default graph available")
         case analysisException: org.apache.spark.sql.AnalysisException => println("Error: " + analysisException.getMessage())
         case unsupportedOperation: common.exceptions.UnsupportedOperation => println("Error: " + unsupportedOperation.getMessage)
         case matchError: scala.MatchError => println("Error: " + matchError.getMessage())
         case disjunctLabels: algebra.exceptions.DisjunctLabelsException => println("Error: " + disjunctLabels.getMessage)
         case schemaExeption: SchemaException => println("Error: " + schemaExeption.getMessage)
         case indexOutOfBoundsException: IndexOutOfBoundsException => println("Error: Missing parameters")
         case _: Throwable => println("Unexpected exception")
       }
    }

  }

  def registerBasicGraph(gcoreRunner: GcoreRunner): Unit ={
    if(!gcoreRunner.catalog.hasGraph("basic_graph")) {
      if (!gcoreRunner.catalog.hasGraph("basic_graph")) {
        gcoreRunner.catalog.registerGraph(BasicGraph(gcoreRunner.sparkSession))
        val basic_graph = gcoreRunner.catalog.graph("basic_graph")
        SaveGraph().saveGraph(basic_graph, gcoreRunner.catalog.databaseDirectory, gcoreRunner.catalog.hdfs_url, true)
        println("Basic graph created")
      }
      gcoreRunner.catalog.setDefaultGraph("basic_graph")
    }
    else
      println("The basic graph is already registered!")
  }


  def removeGraphFreeGraphDataframe(gcoreRunner: GcoreRunner, graphName : String)={
    val hdfs = FileSystem.get(new Path(gcoreRunner.catalog.databaseDirectory).toUri, new Configuration())
    if(gcoreRunner.catalog.hasGraph(graphName)) {
      val graphDir = gcoreRunner.catalog.databaseDirectory+"/"+graphName
      hdfs.delete(new Path(graphDir),true)
      gcoreRunner.catalog.unregisterGraph(graphName)
    }

  }


  def loadGraphFromJson(gcoreRunner: GcoreRunner, jsonName: String) = {
    val path = new Path(jsonName)
    val hdfs = FileSystem.get(path.toUri, new Configuration())

    var jsonFile = hdfs.open(path)
    var jsonPath = path.getParent.toString
    if(jsonFile != null) {
      val graphSource = new GraphSource(gcoreRunner.sparkSession) {
        override val loadDataFn: String => DataFrame = _ => gcoreRunner.sparkSession.emptyDataFrame
      }
      //TODO Aldana show a message if it cannot register the graph

      val catalog = gcoreRunner.catalog.asInstanceOf[SparkCatalog]
      if(!gcoreRunner.catalog.hasGraph(catalog.graphName(graphSource, jsonFile,jsonPath)) ){
        jsonFile = hdfs.open(path)
        val  graphName = catalog.registerGraph(graphSource, jsonFile,jsonPath)
        SaveGraph().saveGraph(gcoreRunner.catalog.graph(graphName),gcoreRunner.catalog.databaseDirectory, gcoreRunner.catalog.hdfs_url,true)
      }
      else
        println("The graph already exists")
    }
    jsonFile.close()
  }

  def setDefaultGraph( gcoreRunner: GcoreRunner , graphp: String): String =
  {
    val r_graph = graphp.replace("\\c","").trim
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
    val r_graph = graphp.replace("\\r","").trim
    gcoreRunner.catalog.unregisterGraph(r_graph)
  }


  def testDirExist(gcoreRunner: GcoreRunner ,path: String): Boolean = {
    val hadoopConf = new Configuration()
    val fs = FileSystem.get(new URI(gcoreRunner.catalog.hdfs_url),hadoopConf)
    val p = new Path(path);
    fs.exists(p) && fs.getFileStatus(p).isDirectory
  }


  def options: Unit =
  {
    println(
      """
        |Options:
        |\h Help.
        |\b Register basic graph. (Graph for demo purposes)
        | ; Execute a query.        (ex. CONSTRUCT (x) MATCH (x);)
        |\c Set default graph.      (\c graph name)
        |\l Show graphs in the database.
        |\s Show graph schema.      (\s graph name)
        |\i Import graph from HDFS. (\i file_name.json)
        |\r Remove graph.           (\r graph_name)
        |\v Activate / Deactivate Log.
        |\q Quit.
      """.stripMargin);
  }

  def loadDatabase(gcoreRunner: GcoreRunner, folder:String, hdfs_url: String):Unit=
  {
    val directory = new Directory
    gcoreRunner.catalog.databaseDirectory = folder
    gcoreRunner.catalog.hdfs_url= hdfs_url;
    val uri= folder
    val load = directory.loadDatabase(uri, gcoreRunner.sparkSession, gcoreRunner.catalog, gcoreRunner.catalog.hdfs_url)
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