/*
package  gui


import java.io.File

import compiler.{CompileContext, Compiler, GcoreCompiler}
import javafx.beans.value.ObservableValue
import org.apache.spark.sql.{DataFrame, SparkSession}
import scalafx.application.JFXApp
import scalafx.geometry.{Insets, Orientation, Pos}
import scalafx.scene.{AccessibleRole, Cursor, Scene}
import scalafx.scene.control._
import scalafx.scene.layout._
import scalafx.Includes._
import scalafx.animation.FadeTransition
import scalafx.collections.ObservableBuffer
import scalafx.concurrent.{Task, Worker}
import scalafx.scene.control.Alert.AlertType
import scalafx.scene.effect.DropShadow
import scalafx.scene.input.MouseEvent
import scalafx.stage.{FileChooser, Screen, Stage, StageStyle}
import schema.Catalog
import spark.{Directory, GraphSource, SparkCatalog}
import spark.examples.{CompanyGraph, DummyGraph, PeopleGraph, SocialGraph}

import scala.tools.nsc.interpreter.Results.Result
import javafx.{collections => jfxc, concurrent => jfxr}
import org.apache.log4j.{Level, Logger}
import org.json4s.FileInput
import scalafx.scene.image.{Image, ImageView}
import scalafx.scene.paint.Color
import scalafx.scene.text.Font

object GcoreGUI extends JFXApp {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)


  var gcoreRunner: GcoreRunner = null

  val loadTask = new jfxr.Task[jfxc.ObservableList[GcoreRunner]]() {

    protected def call: jfxc.ObservableList[GcoreRunner] = {
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

      gcoreRunner = newRunner
      gcoreRunner.catalog.registerGraph(DummyGraph(gcoreRunner.sparkSession))
      gcoreRunner.catalog.registerGraph(PeopleGraph(gcoreRunner.sparkSession))
      gcoreRunner.catalog.registerGraph(SocialGraph(gcoreRunner.sparkSession))
      gcoreRunner.catalog.registerGraph(CompanyGraph(gcoreRunner.sparkSession))
      gcoreRunner.catalog.setDefaultGraph("social_graph")
      loadDatabase(gcoreRunner, "defaultDB")
      ObservableBuffer[GcoreRunner]((gcoreRunner))
    }
  }
  Splash.show(new JFXApp.PrimaryStage(), loadTask, showMainStage)
  new Thread(loadTask).start()


  val queryArea = new TextArea()
  val resultArea = new TextArea()
  val resultTabularArea = new TextArea()
  val resultInfo = new TextArea()
  queryArea.setWrapText(true)
  resultArea.setWrapText(true)
  queryArea.setFont(Font.font("Lucida Console", 12))
  resultArea.setFont(Font.font("Lucida Console", 12))
  resultTabularArea.setFont(Font.font("Lucida Console", 12))
  resultInfo.setFont(Font.font("Lucida Console", 12))
  val resultTab = new TabPane()
  resultTab.style= "-fx-background-color: #dddddd;"
  val textResult = new Tab(){
    content = resultArea
    text = "YARSPG"
    closable = false
  }
  resultTab.getTabs.add(textResult)
  val tabularResult = new Tab(){
    content = resultTabularArea
    text = "Tabular"
    closable = false
  }
  resultTab.getTabs.add(tabularResult)
  val tabularInfo = new Tab(){
    content = resultInfo
    text = "Info"
    closable = false
  }
  resultTab.getTabs.add(tabularInfo)

  val runButton = new Button("Run")
  val examplesButton = new Button("Examples")
  val helpButton = new Button("Help")

  val graphList = new ListView[String]()

  val separator = new Separator()
  separator.setOrientation(Orientation.Vertical)

  runButton.getStyleClass().setAll("button", "success")
  examplesButton.getStyleClass().setAll("button", "primary")
  helpButton.getStyleClass().setAll("button", "info")


  private val queryProgress = new ProgressBar {
    prefWidth = 80
  }
  queryProgress.setVisible(false)

  runButton.prefWidth = 80
  examplesButton.prefWidth = 80

  val topLeftButtonsPanel = new HBox(){
    spacing = 5
    children = Seq (helpButton,examplesButton,separator,runButton,queryProgress)

  }
  val topRightButtonsPanel = new HBox(){
    spacing = 5
    children = Seq (helpButton)

  }

  val topButtonsPanel = new BorderPane(){
    left = topLeftButtonsPanel
    right = topRightButtonsPanel
    padding = Insets(10,10,10,10)
    style ="-fx-background-color: #c6c6c6;"
  }


  val queries = ObservableBuffer[String]()
  val historicQueries = new ComboBox[String] {
    maxWidth = Double.PositiveInfinity
    promptText = "Last queries..."
    items = queries
  }

  val labelQuery = new Label("Query")
  val labelResult = new Label("Result")

  labelQuery.setTextFill(Color.Black)
  labelResult.setTextFill(Color.Black)
  labelQuery.setFont(Font.font("Lucida Console", 14))
  labelResult.setFont(Font.font("Lucida Console", 14))

  val queryAndResultPanel = new VBox(){
    padding = Insets(10,10,10,10)
    spacing = 2
    children = Seq (historicQueries,labelQuery,queryArea,labelResult,resultTab)
  }

  //queryAndResultPanel.getStyleClass().setAll("panel","primary")






  /*new ListView[String]() {
    items  = queries
    cellFactory = {
      p => {
        val cell = new ListCell[String]
        cell.textFill = Color.Blue
        cell.cursor = Cursor.Hand
        cell.prefWidth= this.width.toDouble+2
        cell.wrapText = true
        var text = new Text
        cell.item.onChange { (_, _, str) => text = new Text(str) }
        cell.setGraphic(text)
        text.wrappingWidthProperty().bind(cell.widthProperty())
        text.textProperty().bind(cell.itemProperty())

        cell
      }
    }
  }*/

  /*val leftHistoricPanel = new VBox(){
    padding = Insets(10,10,10,10)
    spacing = 2
    children = Seq (new Label("Last queries"),historicQueries)
  }*/


  private def showMainStage() = {

    var rightPane = listGraphPanel()
    val mainStage = new Stage(StageStyle.Decorated) {
      title = "G-Core"
      width = 1200
      height = 450
      scene = new Scene {
        stylesheets= Seq(getClass.getClassLoader.getResource("bootstrap3.css").toExternalForm)
        root = new BorderPane {
          padding = Insets(10, 10, 10, 10)
          top = topButtonsPanel
          center = queryAndResultPanel
          right = listGraphPanel()
          style ="-fx-background-color: #dddddd;"

        }
      }
      runButton.onAction = handle {
        queries.add(0,queryArea.getText)

        if(queries.size> 10)
          queries.remove(queries.size-1)

        runButton.setDisable(true)
        examplesButton.setDisable(true)
        helpButton.setDisable(true)
        queryProgress.setVisible(true)


        val task = new javafx.concurrent.Task[Unit] {
          override def call(): Unit = {
            gcoreRunner.compiler.compile(queryArea.getText)
          }
          override def succeeded(): Unit = {
            runButton.setDisable(false)
            examplesButton.setDisable(false)
            helpButton.setDisable(false)
            queryProgress.setVisible(false)
            graphList.setItems(ObservableBuffer[String](gcoreRunner.catalog.allGraphsKeys))
          }
          override def failed(): Unit = {
            runButton.setDisable(false)
            examplesButton.setDisable(false)
            helpButton.setDisable(false)
            queryProgress.setVisible(false)

            showInfo("G-Core: Error","Error","The query could not be processed\n"+ this.getException())
          }
        }
        val t = new Thread(task, "Run Query")
        t.setDaemon(true)
        t.start()

      }


      historicQueries.handleEvent(MouseEvent.Any) {
        me: MouseEvent => {
          if (me.getClickCount == 1)
            queryArea.setText(historicQueries.getSelectionModel.getSelectedItem)
        }
      }

      import javafx.beans.value.ChangeListener

      historicQueries.getSelectionModel.selectedItemProperty.addListener(new ChangeListener[String]() {
        override def changed(observable: ObservableValue[_ <: String], oldValue: String, newValue: String): Unit = {
          queryArea.setText(newValue)

        }
      })


      examplesButton.onAction = handle {
        listExamples()
      }
      helpButton.onAction = handle {
        helpDialog()
      }

    }



    mainStage.show()

  }

  def listGraphPanel(): VBox ={
    val defaultGraph = gcoreRunner.catalog.defaultGraph().graphName
    val graphs = ObservableBuffer[String](gcoreRunner.catalog.allGraphsKeys)
    graphList.items= graphs



    val setDefaultButton = new Button("Set default")
    val graphInfoButton = new Button("Graph info")
    val loadGraphButton = new Button("Load graph")

    graphInfoButton.getStyleClass().setAll("button","info")
    setDefaultButton.getStyleClass().setAll("button","warning")
    loadGraphButton.getStyleClass().setAll("button", "primary")

    val bottomButtonsPanel = new HBox(){
      spacing = 5
      children = Seq (graphInfoButton,setDefaultButton,loadGraphButton)

    }
    val defaultGraphLabel = new Label("Available graphs (Default graph: "+ defaultGraph+")")

    val panel : VBox = new VBox(){
      spacing = 2
      padding = Insets(10,10,10,10)
      children = Seq (defaultGraphLabel,graphList,bottomButtonsPanel )
    }

    graphInfoButton.onAction = handle{
      val selectedGraph = graphList.getSelectionModel.getSelectedItem
      showInfo("G-Core: Graph information","Graph information",gcoreRunner.catalog.graph(selectedGraph).toString)
    }

    setDefaultButton.onAction = handle{
      val selectedGraph = graphList.getSelectionModel.getSelectedItem
      gcoreRunner.catalog.setDefaultGraph(selectedGraph)
      defaultGraphLabel.setText("Available graphs (Default graph: "+ selectedGraph+")")
      showInfo("G-Core: Default Graph","Default Graph","The default graph was set to "+selectedGraph)
    }

    loadGraphButton.onAction = handle{
      val fileChooser: FileChooser = new FileChooser()
      val jsonFile: File = fileChooser.showOpenDialog(null)
      if(jsonFile != null){
        val graphSource = new GraphSource(gcoreRunner.sparkSession) {
          override val loadDataFn: String => DataFrame = _ => gcoreRunner.sparkSession.emptyDataFrame
        }

        //TODO Aldana show a message if it cannot register the graph
        gcoreRunner.catalog.asInstanceOf[SparkCatalog].registerGraph(graphSource, jsonFile)
        graphList.setItems(ObservableBuffer[String](gcoreRunner.catalog.allGraphsKeys))
      }
    }

    panel

  }

  def showInfo(dtitle: String, dheaderText: String, graphInfo: String):Unit=
  {
    var alert = new Alert(AlertType.Information) {
      initOwner(stage)
      title = dtitle
      headerText = dheaderText
      contentText = graphInfo

    }
    alert.dialogPane().getStylesheets.add(getClass.getClassLoader.getResource("bootstrap3.css").toExternalForm)

    alert.showAndWait()
  }

  def helpDialog(): Unit =
  {
    var alert = new Alert(AlertType.Information) {
      initOwner(stage)
      title = "G-Core: Help"
      headerText = "Help"
      contentText = "Simplest G-CORE queries:\n" +
        "CONSTRUCT (n)\n" +
        "MATCH (n: Person )\n" +
        "   ON social_graph\n" +
        "WHERE n. employer = 'Acme'\n" +
        "\n" +
        "Multi-Graph Queries:\n" +
        "CONSTRUCT (c) < -[: worksAt ] -(n)\n" +
        "MATCH (c: Company ) ON company_graph ,\n" +
        "      (n: Person ) ON social_graph\n" +
        "   WHERE c. name = n. employer" +
        "\n\n" +
        "Views and Optionals:\n" +
        "GRAPH VIEW social_graph1 AS (\n" +
        "CONSTRUCT (n) -[e] - >(m) SET e. nr_messages := COUNT (*)\n" +
        "MATCH (n) -[e: knows ] - >(m)\n" +
        "  OPTIONAL (n) < -[c1 ] -( msg1 : Post | Comment ) ,\n " +
        "           ( msg1 ) -[: reply_of ] -( msg2 ) ,\n" +
        "           ( msg2 : Post | Comment ) -[c2] - >(m)\n" +
        "     WHERE (c1: has_creator ) AND (c2: has_creator ) )"+
        "  WHERE (n: Person ) AND (m: Person )\n\n" +
        "Create graph:\n" +
        "CREATE GRAPH 'people' " +
        " CONSTRUCT (n)\n" +
        " MATCH (n: Person )\n" +
        "   ON social_graph\n" +
        "   WHERE n. employer = 'Acme'\n"
    }

    alert.dialogPane().getStylesheets.add(getClass.getClassLoader.getResource("bootstrap3.css").toExternalForm)
    alert.showAndWait()
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

  def listExamples(): Unit ={
    val openButton = new ButtonType("Open")
    val dialog = new Dialog[Result]() {
      initOwner(stage)
      title = "G-Core: Examples"
      headerText = "Examples"
    }
    dialog.dialogPane().getStylesheets.add(getClass.getClassLoader.getResource("bootstrap3.css").toExternalForm)
    dialog.dialogPane().buttonTypes = Seq(openButton,ButtonType.Close)
    val exampleQueries = ObservableBuffer[String](
      "CONSTRUCT (n) MATCH (n:Person) ON social_graph" ,
      "CONSTRUCT (n)-[e:E]->(x:X) MATCH (n:Person) ON social_graph",
      "CONSTRUCT (x:X {name := n.firstName})-[e:E]->(n) MATCH (n:Person) ON social_graph",
      "CONSTRUCT (n)-[e:E]->(x GROUP  n.employer :X  {emp := n.employer}) MATCH (n:Person) ON social_graph",
      "CONSTRUCT (m)   MATCH (n:Person)-/<:Knows*>/-> (m:Person) ON social_graph   WHERE  n.firstName = 'John' AND n.lastName = 'Doe'",
      "CONSTRUCT (c) MATCH (c:Company) ON company_graph",
      "CONSTRUCT (c) MATCH (c:Company) ON company_graph WHERE c.name = 'Acme'",
      "CONSTRUCT (c)<-[:worksAt]-(n)  MATCH (c:Company) ON company_graph,  (n:Person)  ON social_graph WHERE c.name = n.employer",
      "CONSTRUCT (c) MATCH (c:Cat) ON dummy_graph",
      "CONSTRUCT (f)<-[:Eats]-(c)  MATCH (c:Cat) ON dummy_graph,  (f:Food) ON dummy_graph",
      "CONSTRUCT (p)<-[:MadeIn]-(f)<-[:Eats]-(c)  MATCH (p:Country)<-[:MadeIn]-(f:Food)<-[:Eats]-(c:Cat) ON dummy_graph where p.name = 'France'",
      "CONSTRUCT (m)   MATCH (n:Cat)-/<:Friend*>/-> (m:Cat) ON dummy_graph   WHERE  n.name = 'Coby'",
      "CONSTRUCT (p) MATCH (p:Person) ON people_graph",
      "CONSTRUCT (c)<-[:worksAt]-(n)  MATCH (c:Company) ON company_graph, (n:Person) ON people_graph  WHERE c.name = n.employer"

    )
    val exampleList = new ListView[String]()
    {
      items  = exampleQueries
    }

    exampleList.setMinWidth(450)
    val graphsListPanel = new VBox(){
      padding = Insets(10,10,10,10)
      children = Seq (exampleList)
    }


    val panel = new HBox(){
      spacing = 5
      padding = Insets(10,10,10,10)
      children = Seq (graphsListPanel)
    }

    /*exampleList.handleEvent(MouseEvent.Any) {
      me: MouseEvent => {
        if (me.getClickCount == 2)
          {
            queryArea.setText(exampleList.getSelectionModel.getSelectedItem)
            dialog.close()
          }
      }
    }*/



    dialog.dialogPane().content()= panel
    dialog.setWidth(450)
    val result = dialog.showAndWait()

    result match {
      case Some(openButton) => {
        queryArea.setText(exampleList.getSelectionModel.getSelectedItem)
        dialog.close()
      }
      case _ => dialog.close()
    }

  }

}

object Splash {


  private val SplashImage = getClass.getClassLoader.getResourceAsStream("ldbc.png")

  private val SplashWidth = 227
  private val SplashHeight = 227

  private val splash = new ImageView(new Image(SplashImage))
  private val loadProgress = new ProgressBar {
    prefWidth = SplashWidth
  }

  private val splashLayout = new VBox() {
    children ++= Seq(splash, loadProgress)
    /*style =
      "-fx-padding: 5; " +
        "-fx-background-color: cornsilk; " +
        "-fx-border-width:5; " +
        "-fx-border-color: " +
        "linear-gradient(" +
        "to bottom, " +
        "chocolate, " +
        "derive(chocolate, 50%)" +
        ");"*/
    effect = new DropShadow()
  }

  def show(splashStage: Stage, loaderTask: Task[_], onSuccess: () => Unit): Unit = {
    loadProgress.progress <== loaderTask.progress
    loaderTask.state.onChange { (_, _, newState) =>
      newState match {
        case Worker.State.Succeeded.delegate =>
          loadProgress.progress.unbind()
          loadProgress.progress = 1
          new FadeTransition(0.5.s, splashLayout) {
            fromValue = 1.0
            toValue = 0.0
            onFinished = handle {
              splashStage.hide()
            }
          }.play()

          onSuccess()

        case _ =>
        // TODO: handle other states
      }
    }


    splashStage.initStyle(StageStyle.Undecorated)
    splashStage.alwaysOnTop = true
    splashStage.scene = new Scene(splashLayout)
    val bounds = Screen.primary.bounds
    splashStage.x = bounds.minX + bounds.width / 2 - SplashWidth / 2
    splashStage.y = bounds.minY + bounds.height / 2 - SplashHeight / 2
  }






}


case class GcoreRunner(sparkSession: SparkSession, compiler: Compiler, catalog: Catalog)

 */