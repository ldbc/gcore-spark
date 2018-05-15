package compiler

import algebra.trees.AlgebraTreeNode
import schema.PathPropertyGraph

/**
  * A step in the compilation process of a G-CORE query. Extending [[Function1]] makes any
  * [[CompilationStage]] composable with another type-compatible [[CompilationStage]].
  */
trait CompilationStage[I, O] extends ((I) => O) {

  /**
    * Defines the behavior of this [[CompilationStage]]. Should be implemented by any extending
    * class.
    */
  def runStage(input: I): O

  override def apply(input: I): O = runStage(input)
}

/**
  * The step in the compilation pipeline that takes in the [[String]] query and produces the
  * algebraic tree.
  */
trait ParseStage extends CompilationStage[String, AlgebraTreeNode] {

  def parse(query: String): AlgebraTreeNode

  override def runStage(input: String): AlgebraTreeNode = parse(input)
}

/**
  * The step in the compilation pipeline that applies rewriting rules over the algebraic tree of
  * the received query. The key idea is to bring the algebraic tree to a state from which a logical
  * plan can be generated.
  */
trait RewriteStage extends CompilationStage[AlgebraTreeNode, AlgebraTreeNode] {

  def rewrite(tree: AlgebraTreeNode): AlgebraTreeNode

  override def runStage(input: AlgebraTreeNode): AlgebraTreeNode = rewrite(input)
}

/**
  * The step in the compilation pipeline that produces a physical plan from the logical plan and
  * then runs it on the target backend.
  */
trait RunTargetCodeStage extends CompilationStage[AlgebraTreeNode, PathPropertyGraph] {

  override def runStage(input: AlgebraTreeNode): PathPropertyGraph
}
