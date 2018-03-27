package algebra.trees

import algebra.expressions.Label
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
        Label("Cat") -> SchemaMap.empty,
        Label("Food") -> SchemaMap.empty,
        Label("Country") -> SchemaMap.empty)))

    override def pathSchema: EntitySchema =
      EntitySchema(SchemaMap(Map(Label("ToGourmand") -> SchemaMap.empty)))

    override def edgeSchema: EntitySchema =
      EntitySchema(SchemaMap(Map(
        Label("Eats") -> SchemaMap.empty,
        Label("Enemy") -> SchemaMap.empty,
        Label("Friend") -> SchemaMap.empty,
        Label("MadeIn") -> SchemaMap.empty)))

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
