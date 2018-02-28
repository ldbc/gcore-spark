package spark

/** An example graph for testing. */
trait TestGraphWrapper {

  val peopleList =
    List(
      Person(id = 100, name = "Daenerys Targaryen", age = 14, isAlive = true),
      Person(id = 101, name = "Tyrion Lannister", age = 25, isAlive = true))

  val cityList =
    List(
      City(id = 102, name = "King's Landing"),
      City(id = 103, name = "Casterly Rock"),
      City(id = 104, name = "Dragonstone"),
      City(id = 105, name = "Pentos"),
      City(id = 106, name = "Vaes Dothrak"),
      City(id = 107, name = "Qarth"),
      City(id = 108, name = "Meeren"))

  val bornInList =
    List(
      BornIn(id = 201, fromId = 100, toId = 104, hasLeft = true),
      BornIn(id = 202, fromId = 101, toId = 103, hasLeft = true))

  val roadList =
    List(
      Road(id = 203, fromId = 103, toId = 102),
      Road(id = 204, fromId = 104, toId = 105),
      Road(id = 205, fromId = 105, toId = 106),
      Road(id = 206, fromId = 106, toId = 107),
      Road(id = 207, fromId = 107, toId = 108))

  val travelRouteList =
    List(
      TravelRoute(id = 301, edges = Seq(203)),
      TravelRoute(id = 302, edges = Seq(204, 205, 206, 207)))
}

sealed case class Person(id: Int, name: String, age: Int, isAlive: Boolean)
sealed case class City(id: Int, name: String)
sealed case class BornIn(id: Int, fromId: Int, toId: Int, hasLeft: Boolean)
sealed case class Road(id: Int, fromId: Int, toId: Int)
sealed case class TravelRoute(id: Int, edges: Seq[Int])
