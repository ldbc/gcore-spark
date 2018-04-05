package spark.sql.operators

/**
  * An SQL query that can additionally need a prologue and an epilogue. The parameter [[resQuery]]
  * should be the query that produces the desired result. The prologue is useful if, for example,
  * temporary views need to be registered before the [[resQuery]], while an epilogue could be
  * useful if, for example, cleanup is needed after the [[resQuery]].
  */
case class SqlQuery(prologue: Seq[String] = Seq.empty,
                    resQuery: String,
                    epilogue: Seq[String] = Seq.empty)
