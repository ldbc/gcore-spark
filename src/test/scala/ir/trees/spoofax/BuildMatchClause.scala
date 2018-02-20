package ir.trees.spoofax

import ir.trees.{SpoofaxTreeBuilder, SpoofaxBaseTreeNode}
import org.scalatest.FunSuite

trait BuildMatchClause extends NodeChecker {
  this: FunSuite =>

  def buildMatchSubtree(): Unit = {

    // MATCH (n)
    test("SpoofaxTreeNode generated from basic MATCH subtree") {
      val query = StrategoHelper.matchTree()

      val tree = SpoofaxTreeBuilder.build(query)
      checkNode(tree, "Match", 2)

      val fullGraphPatternCondition = tree.children.head
      checkNode(fullGraphPatternCondition, "FullGraphPatternCondition", 2)

      val optionals = tree.children(1)
      checkNone(optionals)

      val fullGraphPattern = fullGraphPatternCondition.children.head
      checkNode(fullGraphPattern, "FullGraphPattern", 1)

      val where = fullGraphPatternCondition.children(1)
      checkNone(where)

      val basicGraphPatternLocation = fullGraphPattern.children.head
      checkNode(basicGraphPatternLocation, "BasicGraphPatternLocation", 2)

      val basicGraphPattern = basicGraphPatternLocation.children.head
      checkNode(basicGraphPattern, "BasicGraphPattern", 1)

      val location = basicGraphPatternLocation.children(1)
      checkNone(location)

      val vertex = basicGraphPattern.children.head
      checkNode(vertex, "Vertex", 2)

      val varDef = vertex.children.head
      checkNone(varDef)

      val objMatchPattern = vertex.children(1)
      checkNode(objMatchPattern, "ObjectMatchPattern", 2)

      val labels = objMatchPattern.children.head
      checkNone(labels)

      val properties = objMatchPattern.children(1)
      checkNone(properties)
    }
  }
}
