package algebra.trees

import algebra.expressions.{Label, PropertyKey}
import algebra.types._
import schema._

trait TestGraphWrapper {

  val graphDb: GraphDb = GraphDb.empty

  /**
    * vertex labels: Cat        - name: String, age: Double, weight: Int, onDiet: Boolean
    *                Food       - brand: String
    *                Country    - name: String
    * edge labels:   Eats       - gramsPerDay: Double: (Cat, Food)
    *                Friend     - since: Date: (Cat, Cat)
    *                Enemy      - since: Date, fights: Int: (Cat, Cat)
    *                MadeIn     - (Food, Country)
    * path labels:   ToGourmand - hops: Int
    */
  val catsGraph: PathPropertyGraph = new PathPropertyGraph {
    override def graphName: String = "cats graph"

    override type StorageType = Nothing

    override def vertexSchema: EntitySchema =
      EntitySchema(SchemaMap(Map(
        Label("Cat") -> SchemaMap(Map(
          PropertyKey("id") -> GcoreInteger(),
          PropertyKey("name") -> GcoreString(),
          PropertyKey("age") -> GcoreDecimal(),
          PropertyKey("weight") -> GcoreInteger(),
          PropertyKey("onDiet") -> GcoreBoolean()
        )),
        Label("Food") -> SchemaMap(Map(
          PropertyKey("id") -> GcoreInteger(),
          PropertyKey("brand") -> GcoreString()
        )),
        Label("Country") -> SchemaMap(Map(
          PropertyKey("id") -> GcoreInteger(),
          PropertyKey("name") -> GcoreString()
        ))
      )))

    override def pathSchema: EntitySchema =
      EntitySchema(SchemaMap(Map(
        Label("ToGourmand") -> SchemaMap(Map(
          PropertyKey("id") -> GcoreInteger(),
          PropertyKey("fromId") -> GcoreInteger(),
          PropertyKey("toId") -> GcoreInteger(),
          PropertyKey("edges") -> GcoreArray(),
          PropertyKey("hops") -> GcoreInteger()
        ))
      )))

    override def edgeSchema: EntitySchema =
      EntitySchema(SchemaMap(Map(
        Label("Eats") -> SchemaMap(Map(
          PropertyKey("id") -> GcoreInteger(),
          PropertyKey("fromId") -> GcoreInteger(),
          PropertyKey("toId") -> GcoreInteger(),
          PropertyKey("gramsPerDay") -> GcoreDecimal()
        )),
        Label("Enemy") -> SchemaMap(Map(
          PropertyKey("id") -> GcoreInteger(),
          PropertyKey("fromId") -> GcoreInteger(),
          PropertyKey("toId") -> GcoreInteger(),
          PropertyKey("since") -> GcoreString()
        )),
        Label("Friend") -> SchemaMap(Map(
          PropertyKey("id") -> GcoreInteger(),
          PropertyKey("fromId") -> GcoreInteger(),
          PropertyKey("toId") -> GcoreInteger(),
          PropertyKey("since") -> GcoreString(),
          PropertyKey("fights") -> GcoreString()
        )),
        Label("MadeIn") -> SchemaMap(Map(
          PropertyKey("id") -> GcoreInteger(),
          PropertyKey("fromId") -> GcoreInteger(),
          PropertyKey("toId") -> GcoreInteger()
        ))
      )))

    override def edgeRestrictions: SchemaMap[Label, (Label, Label)] =
      SchemaMap(Map(
        Label("Eats") -> (Label("Cat"), Label("Food")),
        Label("Friend") -> (Label("Cat"), Label("Cat")),
        Label("Enemy") -> (Label("Cat"), Label("Cat")),
        Label("MadeIn") -> (Label("Food"), Label("Country"))))

    override def storedPathRestrictions: SchemaMap[Label, (Label, Label)] =
      SchemaMap(Map(Label("ToGourmand") -> (Label("Cat"), Label("Food"))))

    override def vertexData: Seq[Table[Nothing]] = Seq.empty
    override def edgeData: Seq[Table[Nothing]] = Seq.empty
    override def pathData: Seq[Table[Nothing]] = Seq.empty
  }
}
