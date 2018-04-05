package parser.utils

/** A monotonically increasing counter to create new variable names. */
object VarBinder {
  private var unnamedVarId: Int = -1

  def createVar(prefix: String): String = {
    unnamedVarId += 1
    prefix + "_" + unnamedVarId
  }

  def reset(): Unit = {
    unnamedVarId = -1
  }
}
