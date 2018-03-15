package spark.examples

import algebra.expressions.Label
import org.apache.spark.sql.{DataFrame, SparkSession}
import schema.{SchemaMap, Table}
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
  * 108: Country(name = Germany)
  * 109: Country(name = France)
  *
  * 201: Eats(gramsPerDay = 100) (101 -> 105)
  * 202: Eats(gramsPerDay = 100) (102 -> 107)
  * 203: Eats(gramsPerDay = 80)  (103 -> 106)
  * 204: Eats(gramsPerDay = 60)  (104 -> 107)
  * 205: Friend(since = Dec 2017)(101 -> 102)
  * 206: Friend(since = Dec 2017)(102 -> 101)
  * 207: Enemy(since = Jan 2018) (103 -> 104)
  * 208: Enemy(since = Jan 2018) (104 -> 103)
  * 209: MadeIn() (105 -> 108)
  * 210: MadeIn() (106 -> 108)
  * 211: MadeIn() (107 -> 109)
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
  val germany = Country(108, "Germany")
  val france = Country(109, "France")

  val cobyEatsPurina = Eats(201, 100, 101, 105)
  val hosicoEatsGourmand = Eats(202, 100, 102, 107)
  val kittlerEatsWhiskas = Eats(203, 80, 103, 106)
  val meowsephEatsGourmand = Eats(204, 60, 104, 107)
  val cobyFriendWithHosico = Friend(205, "Dec 2017", 101, 102)
  val hosicoFriendWithCobby = Friend(206, "Dec 2017", 102, 101)
  val kittlerEnemyMeowseph = Enemy(207, "Jan 2018", 103, 104)
  val meowsephEnemyKittler = Enemy(208, "Jan 2018", 104, 103)
  val purinaMadeInGermany = MadeIn(209, 105, 108)
  val whiskasMadeInGermany = MadeIn(210, 106, 108)
  val gourmandMadeInFrance = MadeIn(211, 107, 109)

  val fromCoby = ToGourmand(301, 2, Seq(205, 202))
  val fromHosico = ToGourmand(302, 1, Seq(202))
  val fromKittler = ToGourmand(303, 2, Seq(207, 204))
  val fromMeowseph = ToGourmand(304, 1, Seq(204))

  import spark.implicits._

  override def graphName: String = "dummy_graph"

  override def vertexData: Seq[Table[DataFrame]] =
    Seq(
      Table("Cat", Seq(coby, hosico, kittler, meowseph).toDF()),
      Table("Food", Seq(purina, whiskas, gourmand).toDF()),
      Table("Country", Seq(germany, france).toDF()))

  override def edgeData: Seq[Table[DataFrame]] =
    Seq(
      Table(
        "Eats",
        Seq(cobyEatsPurina, hosicoEatsGourmand, kittlerEatsWhiskas, meowsephEatsGourmand).toDF()),
      Table("Friend", Seq(cobyFriendWithHosico, hosicoFriendWithCobby).toDF()),
      Table("Enemy", Seq(kittlerEnemyMeowseph, meowsephEnemyKittler).toDF()),
      Table("MadeIn", Seq(purinaMadeInGermany, whiskasMadeInGermany, gourmandMadeInFrance).toDF())
    )

  override def pathData: Seq[Table[DataFrame]] =
    Seq(
      Table("ToGourmand", Seq(fromCoby, fromHosico, fromKittler, fromMeowseph).toDF()))

  override def edgeRestrictions: SchemaMap[Label, (Label, Label)] =
    SchemaMap(Map(
      Label("Eats") -> (Label("Cat"), Label("Food")),
      Label("Friend") -> (Label("Cat"), Label("Cat")),
      Label("Enemy") -> (Label("Cat"), Label("Cat")),
      Label("MadeIn") -> (Label("Food"), Label("Country"))
    ))
}

sealed case class Cat(id: Int, name: String, age: Double, weight: Int, onDiet: Boolean)
sealed case class Food(id: Int, brand: String)
sealed case class Country(id: Int, name: String)
sealed case class Eats(id: Int, gramsPerDay: Double, fromId: Int, toId: Int)
sealed case class Friend(id: Int, since: String, fromId: Int, toId: Int)
sealed case class Enemy(id: Int, since: String, fromId: Int, toId: Int)
sealed case class MadeIn(id: Int, fromId: Int, toId: Int)
sealed case class ToGourmand(id: Int, hops: Int, edges: Seq[Int])
