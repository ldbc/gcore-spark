package spark.sql.operators

case class SqlQuery(prologue: Seq[String], resQuery: String, epilogue: Seq[String])
