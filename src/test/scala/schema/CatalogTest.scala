package schema

import algebra.expressions.Label
import org.scalatest.FunSuite

class CatalogTest extends FunSuite {

  val graph1: PartialGraph = new PartialGraph {
    override def graphName: String = "graph1"

    override def edgeRestrictions: SchemaMap[Label, (Label, Label)] = SchemaMap.empty
    override def storedPathRestrictions: SchemaMap[Label, (Label, Label)] = SchemaMap.empty
  }

  val graph2: PartialGraph = new PartialGraph {
    override def graphName: String = "graph2"

    override def edgeRestrictions: SchemaMap[Label, (Label, Label)] = SchemaMap.empty
    override def storedPathRestrictions: SchemaMap[Label, (Label, Label)] = SchemaMap.empty
  }

  test("allGraphs") {
    val catalog: ACatalog = new ACatalog {}
    catalog.registerGraph(graph1)
    catalog.registerGraph(graph2)
    assert(catalog.allGraphs == Seq(graph1, graph2))
  }

  test("hasGraph") {
    val catalog: ACatalog = new ACatalog {}
    catalog.registerGraph(graph1)
    catalog.registerGraph(graph2)
    assert(catalog.hasGraph("graph1"))
    assert(catalog.hasGraph("graph2"))
  }

  test("graph") {
    val catalog: ACatalog = new ACatalog {}
    catalog.registerGraph(graph1)
    catalog.registerGraph(graph2)
    assert(catalog.graph("graph1") == graph1)
    assert(catalog.graph("graph2") == graph2)
  }

  test("unregisterGraph(graphName: String) for non-default graph") {
    val catalog: ACatalog = new ACatalog {}
    catalog.registerGraph(graph1)
    catalog.unregisterGraph("graph1")
    assert(catalog.allGraphs == Seq.empty)
  }

  test("unregisterGraph(graphName: String) for default graph") {
    val catalog: ACatalog = new ACatalog {}
    catalog.registerGraph(graph1)
    catalog.setDefaultGraph("graph1")
    catalog.unregisterGraph("graph1")
    assert(catalog.allGraphs == Seq.empty)
  }

  test("hasDefaultGraph and defaultGraph") {
    val catalog: ACatalog = new ACatalog {}
    catalog.registerGraph(graph1)
    assert(!catalog.hasDefaultGraph)

    catalog.setDefaultGraph("graph1")
    assert(catalog.hasDefaultGraph)
    assert(catalog.defaultGraph() == graph1)

    catalog.unregisterGraph(graph1)
    assert(!catalog.hasDefaultGraph)
  }

  test("setDefaultGraph throws exception if graph is not registered") {
    val catalog: ACatalog = new ACatalog {}
    assert(catalog.allGraphs.isEmpty)

    assertThrows[SchemaException] {
      catalog.setDefaultGraph("graph1")
    }
  }

  test("resetDefaultGraph") {
    val catalog: ACatalog = new ACatalog {}
    catalog.registerGraph(graph1)
    catalog.setDefaultGraph("graph1")
    catalog.resetDefaultGraph()
    assert(catalog.allGraphs == Seq(graph1))
    assert(!catalog.hasDefaultGraph)
  }

  sealed abstract class ACatalog extends Catalog {
    override type StorageType = Nothing
  }

  /**
    * Graphs used in this test suite. We are not interested in schema or data, but it is not the
    * empty graph either.
    */
  sealed abstract class PartialGraph extends PathPropertyGraph {
    override type StorageType = Nothing

    override def vertexSchema: EntitySchema = EntitySchema.empty
    override def pathSchema: EntitySchema = EntitySchema.empty
    override def edgeSchema: EntitySchema = EntitySchema.empty
    override def vertexData: Seq[Table[Nothing]] = Seq.empty
    override def edgeData: Seq[Table[Nothing]] = Seq.empty
    override def pathData: Seq[Table[Nothing]] = Seq.empty
  }
}
