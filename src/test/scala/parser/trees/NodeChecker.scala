package parser.trees

trait NodeChecker {

  def checkNode(node: SpoofaxBaseTreeNode, name: String, numChildren: Int): Unit = {
    assert(node.name == name)
    assert(node.children.lengthCompare(numChildren) == 0)
  }

  def checkNone(node: SpoofaxBaseTreeNode): Unit = {
    checkNode(node, "None", 0)
  }

  def checkLeafValue(node: SpoofaxBaseTreeNode, expectedValue: String): Unit = {
    assert(node.asInstanceOf[SpoofaxLeaf[String]].value == expectedValue)
  }

  def checkConn(node: SpoofaxBaseTreeNode, expectedConn: String, expectedRef: String): Unit = {
    val some = node.children.head
    val edge = some.children.head
    val edgeMatchPattern = edge.children.head
    val someVarDef = edgeMatchPattern.children.head
    val varDef = someVarDef.children.head
    var ref = varDef.children.head
    var objMatchPattern = edgeMatchPattern.children(1)

    checkNode(node, expectedConn, 1)
    checkNode(some, "Some", 1)
    checkNode(edge, "Edge", 1)
    checkNode(edgeMatchPattern, "EdgeMatchPattern", 2)
    checkNode(someVarDef, "Some", 1)
    checkNode(varDef, "VarDef", 1)
    checkLeafValue(ref, expectedRef)
    checkNode(objMatchPattern, "ObjectMatchPattern", 2)
    checkNone(objMatchPattern.children.head)
    checkNone(objMatchPattern.children(1))
  }
}
