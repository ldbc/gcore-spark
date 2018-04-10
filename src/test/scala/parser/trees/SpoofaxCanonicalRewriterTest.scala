package parser.trees

import org.scalatest.{BeforeAndAfterEach, FunSuite, Inside, Matchers}
import parser.utils.VarBinder

class SpoofaxCanonicalRewriterTest extends FunSuite
  with Matchers with Inside with BeforeAndAfterEach with MinimalSpoofaxParser {

  override protected def beforeEach(): Unit = {
    VarBinder.reset()
  }

  /************************************** MATCH ***************************************************/
  test("MATCH () => MATCH (v_0)") {
    val tree = extractVertexMatch("CONSTRUCT (u) MATCH ()")
    verifyVertexIsNamed(tree)
  }

  test("MATCH (n)->(m) => MATCH (n)-[e_0]->(m)") {
    verifyEdgeMatchIsNamed("CONSTRUCT (u) MATCH (n)->(m)", "OutConn")
  }

  test("MATCH (n)<-(m) => MATCH (n)<-[e_0]-(m)") {
    verifyEdgeMatchIsNamed("CONSTRUCT (u) MATCH (n)<-(m)", "InConn")
  }

  test("MATCH (n)<->(m) => MATCH (n)<-[e_0]->(m)") {
    verifyEdgeMatchIsNamed("CONSTRUCT (u) MATCH (n)<->(m)", "InOutEdge")
  }

  test("MATCH (n)-(m) => MATCH (n)-[e_0]-(m)") {
    verifyEdgeMatchIsNamed("CONSTRUCT (u) MATCH (n)-(m)", "UndirectedEdge")
  }

  test("MATCH (n)-->(m) => MATCH (n)-[e_0]->(m)") {
    verifyEdgeMatchIsNamed("CONSTRUCT (u) MATCH (n)-->(m)", "OutConn")
  }

  test("MATCH (n)<--(m) => MATCH (n)<-[e_0]-(m)") {
    verifyEdgeMatchIsNamed("CONSTRUCT (u) MATCH (n)<--(m)", "InConn")
  }

  test("MATCH (n)<-->(m) => MATCH (n)<-[e_0]->(m)") {
    verifyEdgeMatchIsNamed("CONSTRUCT (u) MATCH (n)<-->(m)", "InOutEdge")
  }

  test("MATCH (n)--(m) => MATCH (n)-[e_0]-(m)") {
    verifyEdgeMatchIsNamed("CONSTRUCT (u) MATCH (n)--(m)", "UndirectedEdge")
  }

  test("MATCH (n)-[]->(m) => MATCH (n)-[e_0]->(m)") {
    verifyEdgeMatchIsNamed("CONSTRUCT (u) MATCH (n)-[]->(m)", "OutConn")
  }

  test("MATCH (n)<-[]-(m) => MATCH (n)<-[e_0]-(m)") {
    verifyEdgeMatchIsNamed("CONSTRUCT (u) MATCH (n)<-[]-(m)", "InConn")
  }

  test("MATCH (n)<-[]->(m) => MATCH (n)<-[e_0]->(m)") {
    verifyEdgeMatchIsNamed("CONSTRUCT (u) MATCH (n)<-[]->(m)", "InOutEdge")
  }

  test("MATCH (n)-[]-(m) => MATCH (n)-[e_0]-(m)") {
    verifyEdgeMatchIsNamed("CONSTRUCT (u) MATCH (n)-[]-(m)", "UndirectedEdge")
  }

  test("MATCH (n)-/ /->(m) => MATCH (n)-/ /->(m) (do not bind unnamed path)") {
    val connTree = extractConnectionMatch("CONSTRUCT (u) MATCH (n)-/ /-(m)")

    inside(connTree) {
      case SpoofaxBaseTreeNode("UndirectedEdge") =>
        val some = connTree.children.head
        inside(some) {
          case SpoofaxBaseTreeNode("Some") =>
            val path = some.children.head
            inside(path) {
              case SpoofaxBaseTreeNode("Path") =>
                val virtual = path.children.head
                inside(virtual) {
                  case SpoofaxBaseTreeNode("Virtual") =>
                    val varDef = virtual.children.head
                    varDef should matchPattern {
                      case SpoofaxBaseTreeNode("None") =>
                    }
                }
            }
        }
    }
  }

  /************************************* CONSTRUCT ************************************************/
  /**
    * CONSTRUCT clause is only defined on connections of type ->, <- and <->. Undirected connections
    * are not defined in the grammar:
    * https://github.com/ldbc/ldbc_gcore_parser/blob/master/gcore-spoofax/syntax/ConstructPattern.sdf3
    * However, Spoofax parses undirected connections as out connections (->). We do not add any
    * tests for this here.
    *
    * Also, as per the grammar, virtual paths should always be named:
    * EdgeOrPathContentConstructPattern.PathVirtual 		= </<VarRef>/>
    * We will not test virtual path behavior below.
    */

  test("CONSTRUCT () => CONSTRUCT (v_0)") {
    val tree = extractVertexConstruct("CONSTRUCT () MATCH (u)")
    verifyVertexIsNamed(tree)
  }

  test("CONSTRUCT (n)->(m) => CONSTRUCT (n)-[e_0]->(m)") {
    verifyEdgeConstructIsNamed("CONSTRUCT (n)->(m) MATCH (u)", "OutConn")
  }

  test("CONSTRUCT (n)<-(m) => CONSTRUCT (n)<-[e_0]-(m)") {
    verifyEdgeConstructIsNamed("CONSTRUCT (n)<-(m) MATCH (u)", "InConn")
  }

  test("CONSTRUCT (n)<->(m) => CONSTRUCT (n)<-[e_0]->(m)") {
    verifyEdgeConstructIsNamed("CONSTRUCT (n)<->(m) MATCH (u)", "InOutEdge")
  }

  test("CONSTRUCT (n)-->(m) => CONSTRUCT (n)-[e_0]->(m)") {
    verifyEdgeConstructIsNamed("CONSTRUCT (n)-->(m) MATCH (u)", "OutConn")
  }

  test("CONSTRUCT (n)<--(m) => CONSTRUCT (n)<-[e_0]-(m)") {
    verifyEdgeConstructIsNamed("CONSTRUCT (n)<--(m) MATCH (u)", "InConn")
  }

  test("CONSTRUCT (n)<-->(m) => CONSTRUCT (n)<-[e_0]->(m)") {
    verifyEdgeConstructIsNamed("CONSTRUCT (n)<-->(m) MATCH (u)", "InOutEdge")
  }

  test("CONSTRUCT (n)-[]->(m) => CONSTRUCT (n)-[e_0]->(m)") {
    verifyEdgeConstructIsNamed("CONSTRUCT (n)-[]->(m) MATCH (u)", "OutConn")
  }

  test("CONSTRUCT (n)<-[]-(m) => CONSTRUCT (n)<-[e_0]-(m)") {
    verifyEdgeConstructIsNamed("CONSTRUCT (n)<-[]-(m) MATCH (u)", "InConn")
  }

  test("CONSTRUCT (n)<-[]->(m) => CONSTRUCT (n)<-[e_0]->(m)") {
    verifyEdgeConstructIsNamed("CONSTRUCT (n)<-[]->(m) MATCH (u)", "InOutEdge")
  }

  test("CONSTRUCT (n)-/@/->(m) => CONSTRUCT (n)-/@p_0/->(m)") {
    verifyPathObjConstructIsNamed("CONSTRUCT (n)-/@/->(m) MATCH (u)", "OutConn")
  }

  test("CONSTRUCT (n)<-/@/-(m) => CONSTRUCT (n)<-/@p_0/-(m)") {
    verifyPathObjConstructIsNamed("CONSTRUCT (n)<-/@/-(m) MATCH (u)", "InConn")
  }

  /*************************************** helpers ************************************************/
  private def extractVertexMatch(query: String): SpoofaxBaseTreeNode = {
    val tree = canonicalize(query)
    val matchClause = tree.children(2)
    val fullGraphPatternCondition = matchClause.children.head
    val fullGraphPattern = fullGraphPatternCondition.children.head
    val basicGraphPatternLocation = fullGraphPattern.children.head
    val basicGraphPattern = basicGraphPatternLocation.children.head
    val vertexPattern = basicGraphPattern.children.head
    vertexPattern
  }

  private def extractVertexConstruct(query: String): SpoofaxBaseTreeNode = {
    val tree = canonicalize(query)
    val constructClause = tree.children(1)
    val constructPattern = constructClause.children.head
    val basicConstructPattern = constructPattern.children.head
    val vertexPattern = basicConstructPattern.children.head
    vertexPattern
  }

  private def extractConnectionMatch(query: String): SpoofaxBaseTreeNode = {
    val tree = canonicalize(query)
    val matchClause = tree.children(2)
    val fullGraphPatternCondition = matchClause.children.head
    val fullGraphPattern = fullGraphPatternCondition.children.head
    val basicGraphPatternLocation = fullGraphPattern.children.head
    val basicGraphPattern = basicGraphPatternLocation.children.head
    val edgeVertexMatchPattern = basicGraphPattern.children(1)
    val connection = edgeVertexMatchPattern.children.head
    connection
  }

  private def extractConnectionConstruct(query: String): SpoofaxBaseTreeNode = {
    val tree = canonicalize(query)
    val constructClause = tree.children(1)
    val constructPattern = constructClause.children.head
    val basicConstructPattern = constructPattern.children.head
    val edgeVertexConstructPattern = basicConstructPattern.children(1)
    val connection = edgeVertexConstructPattern.children.head
    connection
  }

  private def verifyVertexIsNamed(tree: SpoofaxBaseTreeNode): Unit = {
    inside(tree) {
      case SpoofaxBaseTreeNode("Vertex") =>
        val some = tree.children.head
        inside(some) {
          case SpoofaxBaseTreeNode("Some") =>
            val varDef = some.children.head
            inside(varDef) {
              case SpoofaxBaseTreeNode("VarDef") =>
                assert(varDef.children.head.asInstanceOf[SpoofaxLeaf[String]].value == "v_0")
            }
        }
    }
  }

  private def verifyEdgeMatchIsNamed(query: String, expectedConn: String): Unit = {
    val tree = extractConnectionMatch(query)
    verifyEdgeOrIsNamed(tree, expectedConn, "EdgeMatchPattern")
  }

  private def verifyEdgeConstructIsNamed(query: String, expectedConn: String): Unit = {
    val tree = extractConnectionConstruct(query)
    verifyEdgeOrIsNamed(tree, expectedConn,  "EdgeConstructPattern")
  }

  private def verifyPathObjConstructIsNamed(query: String, expectedConn: String): Unit = {
    val connTree = extractConnectionConstruct(query)
    inside(connTree) {
      case SpoofaxBaseTreeNode(`expectedConn`) =>
        val some = connTree.children.head
        inside(some) {
          case SpoofaxBaseTreeNode("Some") =>
            val path = some.children.head
            inside(path) {
              case SpoofaxBaseTreeNode("PathObjectified") =>
                val someVarDef = path.children.head
                inside(someVarDef) {
                  case SpoofaxBaseTreeNode("Some") =>
                    val varDef = someVarDef.children.head
                    inside(varDef) {
                      case SpoofaxBaseTreeNode("VarRefDef") =>
                        assert(
                          varDef.children.head.asInstanceOf[SpoofaxLeaf[String]].value == "p_0")
                    }
                }
            }
        }
    }
  }

  private def verifyEdgeOrIsNamed(connTree: SpoofaxBaseTreeNode,
                                      expectedConn: String,
                                      expectedPatternType: String): Unit = {
    inside(connTree) {
      case SpoofaxBaseTreeNode(`expectedConn`) =>
        val some = connTree.children.head
        inside(some) {
          case SpoofaxBaseTreeNode("Some") =>
            val edge = some.children.head
            inside(edge) {
              case SpoofaxBaseTreeNode("Edge") =>
                val edgeMatchPattern = edge.children.head
                inside(edgeMatchPattern) {
                  case SpoofaxBaseTreeNode(`expectedPatternType`) =>
                    val someVarDef = edgeMatchPattern.children.head
                    inside(someVarDef) {
                      case SpoofaxBaseTreeNode("Some") =>
                        val varDef = someVarDef.children.head
                        inside(varDef) {
                          case SpoofaxBaseTreeNode("VarRefDef") =>
                            assert(
                              varDef.children.head.asInstanceOf[SpoofaxLeaf[String]].value == "e_0")
                        }
                    }
                }
            }
        }
    }
  }
}
