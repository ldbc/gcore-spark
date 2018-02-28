package spark.examples

import org.apache.spark.sql.{DataFrame, SparkSession}
import schema.Table
import spark._

/**
  * An example schema to experiment with.
  *
  * vertex labels: Cat        - name: String, age: Double, weight: Int, onDiet: Boolean
  *                Food       - brand: String
  * edge labels:   Eats       - gramsPerDay: Double: (Cat, Food)
  *                Friend     - since: Date: (Cat, Cat)
  *                Enemy      - since: Date: (Cat, Cat)
  * path labels:   ToGourmand - hops: Int
  *
  * Graph:
  * 101: Cat(name = Coby, age = 3, weight = 6, onDiet = false)
  * 102: Cat(name = Hosico, age = 4, weight = 7, onDiet = true)
  * 103: Cat(name = Kittler, age = 8, weight = 8, onDiet = true)
  * 104: Cat(name = Meowseph, age = 0.5, weight = 2, onDiet = false)
  * 105: Food(brand = Purina)
  * 106: Food(brand = Whiskas)
  * 107: Food(brand = Gourmand)
  *
  * 201: Eats(gramsPerDay = 100) (101 -> 105)
  * 202: Eats(gramsPerDay = 100) (102 -> 107)
  * 203: Eats(gramsPerDay = 80)  (103 -> 106)
  * 204: Eats(gramsPerDay = 60)  (104 -> 107)
  * 205: Friend(since = Dec 2017)(101 -> 102)
  * 206: Friend(since = Dec 2017)(102 -> 101)
  * 207: Enemy(since = Jan 2018) (103 -> 104)
  * 208: Enemy(since = Jan 2018) (104 -> 103)
  *
  * 301: ToGourmand(hops = 2)(205, 202)
  * 302: ToGourmand(hops = 1)(202)
  * 303: ToGourmand(hops = 2)(207, 204)
  * 304: ToGourmand(hops = 1)(204)
  */
final case class DummyGraph(spark: SparkSession) extends SparkGraph {
  val coby = Cat(101, "Coby", 3, 6, onDiet = false)
  val hosico = Cat(102, "Hosico", 4, 7, onDiet = true)
  val kittler = Cat(103, "Kittler", 8, 8, onDiet = true)
  val meowseph = Cat(104, "Meowseph", 0.5, 2, onDiet = false)
  val purina = Food(105, "Purina")
  val whiskas = Food(106, "Whiskas")
  val gourmand = Food(107, "Gourmand")

  val cobyEatsPurina = Eats(201, 100, 101, 105)
  val hosicoEatsGourmand = Eats(202, 100, 102, 107)
  val kittlerEatsWhiskas = Eats(203, 80, 103, 106)
  val meowsephEatsGourmand = Eats(204, 60, 104, 107)
  val cobyFriendWithHosico = Friend(205, "Dec 2017", 101, 102)
  val hosicoFriendWithCobby = Friend(206, "Dec 2017", 102, 101)
  val kittlerEnemyMeowseph = Enemy(207, "Jan 2018", 103, 104)
  val meowsephEnemyKittler = Enemy(208, "Jan 2018", 104, 103)

  val fromCoby = ToGourmand(301, 2, Seq(205, 202))
  val fromHosico = ToGourmand(302, 1, Seq(202))
  val fromKittler = ToGourmand(303, 2, Seq(207, 204))
  val fromMeowseph = ToGourmand(304, 1, Seq(204))

  import spark.implicits._

  override def graphName: String = "dummy_graph"

  override def vertexData: Seq[Table[DataFrame]] =
    Seq(
      Table("Cat", Seq(coby, hosico, kittler, meowseph).toDF()),
      Table("Food", Seq(purina, whiskas, gourmand).toDF()))

  override def edgeData: Seq[Table[DataFrame]] =
    Seq(
      Table("Eats", Seq(cobyEatsPurina, hosicoEatsGourmand, kittlerEatsWhiskas, meowsephEatsGourmand)
        .toDF()),
      Table("Friend", Seq(cobyFriendWithHosico, hosicoFriendWithCobby).toDF()),
      Table("Enemy", Seq(kittlerEnemyMeowseph, meowsephEnemyKittler).toDF()))

  override def pathData: Seq[Table[DataFrame]] =
    Seq(
      Table("ToGourmand", Seq(fromCoby, fromHosico, fromKittler, fromMeowseph).toDF()))
}

sealed case class Cat(id: Int, name: String, age: Double, weight: Int, onDiet: Boolean)
sealed case class Food(id: Int, brand: String)
sealed case class Eats(id: Int, gramsPerDay: Double, fromId: Int, toId: Int)
sealed case class Friend(id: Int, since: String, fromId: Int, toId: Int)
sealed case class Enemy(id: Int, since: String, fromId: Int, toId: Int)
sealed case class ToGourmand(id: Int, hops: Int, edges: Seq[Int])
