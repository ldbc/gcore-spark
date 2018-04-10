package algebra.types

/** Where does a connection, edge or path, point? */
abstract class ConnectionType extends AlgebraType

/**
  * A connection with the arrow pointing towards the left hand-side of it, as the query is read from
  * left to right. For example, the connection (u)<-(v) is an [[InConn]]ection.
  */
case object InConn extends ConnectionType

/**
  * A connection with the arrow pointing towards the right hand-side of it, as the query is read
  * from left to right. For example, the connection (u)->(v) is an [[OutConn]]ection.
  */
case object OutConn extends ConnectionType

/**
  * A connection with the arrow pointing towards both sides of it, as the query is read from left to
  * right. For example, the connection (u)<->(v) is an [[InOutConn]]ection.
  */
case object InOutConn extends ConnectionType

/**
  * A connection in which there is no arrow. For example, the connection (u)-(v) is an
  * [[UndirectedConn]]ection.
  */
case object UndirectedConn extends ConnectionType
