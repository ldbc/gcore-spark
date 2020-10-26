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
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import schema.{Catalog, SchemaException}
import spark.{Directory, GraphSource, SaveGraph, SparkCatalog}
import spark.examples.{BasicGraph, CompanyGraph, DummyGraph, PeopleGraph, SocialGraph}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel
import parser.SpoofaxParser
import scala.collection.mutable.{ArrayBuffer, ListBuffer}



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
    var freeGraphs: Seq[String] = null
    val hdfs_url= "hdfs://localhost:9000/";
    if(args.size > 0 && args.head == "-d"){
      loadDatabase(gcoreRunner, hdfs_url+"example/"+args.last,"hdfs://localhost:9000/")
      freeGraphs = loadRegisteredFreeGraphs(gcoreRunner)
    }
    else{
      //ip-172-31-77-252.ec2.internal:8020
      loadDatabase(gcoreRunner, hdfs_url+"example/defaultDB",hdfs_url)
      freeGraphs= loadRegisteredFreeGraphs(gcoreRunner)
    }

    if(!gcoreRunner.catalog.hasGraph("basic_graph")){
      gcoreRunner.catalog.registerGraph(BasicGraph(gcoreRunner.sparkSession))
      val basic_graph = gcoreRunner.catalog.graph("basic_graph")
      SaveGraph().saveJsonGraph(basic_graph,gcoreRunner.catalog.databaseDirectory, gcoreRunner.catalog.hdfs_url)
      println("Basic graph created in HDFS")
    }

    gcoreRunner.catalog.setDefaultGraph("basic_graph")
    freeGraphs.foreach(uri =>{
      loadGraphFromJson(gcoreRunner,uri,false)
    })


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
        case "\\i" =>
          val dir = gcoreRunner.catalog.databaseDirectory+"/free_graph"
          loadGraphFromJson(gcoreRunner, line.replace("\\i","").trim,true)
        case "\\h" =>
          options
        case "\\l" =>
          println(gcoreRunner.catalog.toString)
        case "\\s" =>
          println(gcoreRunner.catalog.graph(line.split(" ")(1)).toString)
        case "\\0" =>
          showFreeGraphDataframe(gcoreRunner)
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

  def showFreeGraphDataframe(gcoreRunner: GcoreRunner) ={
    val df= readFreeGraphDataframe(gcoreRunner)
    if(df != null)
      df.show(false)
  }
  def removeGraphFreeGraphDataframe(gcoreRunner: GcoreRunner, graphName : String)={
    val df = readFreeGraphDataframe(gcoreRunner)
    val dir = gcoreRunner.catalog.databaseDirectory+"/free_graph"
    val dir_t = gcoreRunner.catalog.databaseDirectory+"/temp_free_graph"
    val path = new Path(dir)
    val path_t = new Path(dir_t)
    val hdfs = FileSystem.get(new URI(gcoreRunner.catalog.hdfs_url), new Configuration())
    if(df != null) {
      if(df.filter(col("graphName").isin((graphName))).count()>0) {
        val df2 = df.filter(!col("graphName").isin((graphName)))
        df2.write.format("json").mode(SaveMode.Overwrite).save(dir_t)
        hdfs.delete(path, true)
        hdfs.rename(path_t, path)
        df2.cache()
        gcoreRunner.catalog.unregisterGraph(graphName)
      }
      else if(gcoreRunner.catalog.hasGraph(graphName)) {
        val graphDir = gcoreRunner.catalog.databaseDirectory+"/"+graphName
        hdfs.delete(new Path(graphDir),true)
        gcoreRunner.catalog.unregisterGraph(graphName)
      }

    }
  }

  def readFreeGraphDataframe(gcoreRunner: GcoreRunner):DataFrame ={
    var df:DataFrame =null
    val dir = gcoreRunner.catalog.databaseDirectory+"/free_graph"
    if (testDirExist(gcoreRunner,dir))
      try
        df = gcoreRunner.sparkSession.sqlContext.read.json(dir)
    df
  }

  def loadGraphFromJson(gcoreRunner: GcoreRunner, jsonName: String, new_graph: Boolean):DataFrame = {

    val dir = gcoreRunner.catalog.databaseDirectory+"/free_graph"
    val hdfs = FileSystem.get(new URI(gcoreRunner.catalog.hdfs_url), new Configuration())
    val path = new Path(jsonName)
    val jsonFile = hdfs.open(path)
    var newRow:DataFrame = null
    if(jsonFile != null) {
      val graphSource = new GraphSource(gcoreRunner.sparkSession) {
        override val loadDataFn: String => DataFrame = _ => gcoreRunner.sparkSession.emptyDataFrame
      }
      //TODO Aldana show a message if it cannot register the graph
      val  graphName = gcoreRunner.catalog.asInstanceOf[SparkCatalog].registerGraph(graphSource, jsonFile)
      var id = 1;
      val df = readFreeGraphDataframe(gcoreRunner)
      var notExist:Boolean = true
      if(df != null) {
        if(df.columns.size > 0){
          id= Integer.valueOf(df.select("id").sort(desc("id")).first().get(0).toString)+1
          notExist = df.select("uri").filter(col("uri") === jsonName).count() == 0
        }
      }
      val seq = Seq((id, jsonName,graphName))
      newRow = gcoreRunner.sparkSession.createDataFrame(seq).toDF("id", "uri","graphName")
      if(new_graph && notExist)
        newRow.write.format("json").mode(SaveMode.Append).save(dir)
    }
    gcoreRunner.sparkSession.catalog.refreshByPath(dir)
    newRow

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
    var r_graph = graphp.replace("\\r","").trim
    gcoreRunner.catalog.unregisterGraph(r_graph)
  }
  def loadRegisteredFreeGraphs(gcoreRunner: GcoreRunner) :Seq[String] = {
    var seq = Seq[String]()
    val dir = gcoreRunner.catalog.databaseDirectory+"/free_graph"
    var df:DataFrame = null
    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("uri", StringType, true),
        StructField("graphName", StringType, true)
      )
    )
    if (testDirExist(gcoreRunner,dir)) {
    {
      try {
        df = gcoreRunner.sparkSession.sqlContext.read.json(dir)
        df.show(false)
      }
      catch {
        case a:AnalysisException =>
          df= gcoreRunner.sparkSession.createDataFrame(gcoreRunner.sparkSession.sparkContext.emptyRDD[Row], schema)
      }
    }

    }
    if (df != null){
      if(df.columns.size > 0)
        seq = df.select("uri").rdd.map(r => r(0).toString).collect().toSeq
    }
    seq
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
        | ; Execute a query.        (ex. CONSTRUCT (x) MATCH (x);)
        |\c Set default graph.      (\c graph name)
        |\l Show graphs in the database.
        |\s Show graph schema.      (\d graph name)
        |\i Import graph from HDFS. (\g file_name.json)
        |\r Remove graph.           (\r id)
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