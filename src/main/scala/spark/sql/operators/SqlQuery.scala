package spark.sql.operators

case class SqlQuery(prologue: Seq[String] = Seq.empty,
                    resQuery: String,
                    epilogue: Seq[String] = Seq.empty)
