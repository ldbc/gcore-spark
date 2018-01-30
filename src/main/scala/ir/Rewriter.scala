package ir

/** Rewrites input of type I to output of type O. I and O can be the same type. */
trait Rewriter[I, O] {
  def process(input: I): O
}
