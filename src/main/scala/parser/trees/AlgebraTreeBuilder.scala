package parser.trees

import algebra.expressions._
import algebra.operators._
import algebra.types._
import algebra.trees.AlgebraTreeNode
import common.trees.Builder
import parser.exceptions.QueryParseException

/**
  * Creates the algebraic tree from the lexical tree generated with Spoofax.
  *
  * The root of the algebra tree is a [[Query]] clause, whose children are the optional path clause
  * and the mandatory construct clause and [[MatchClause]].
  */
object AlgebraTreeBuilder extends Builder[SpoofaxBaseTreeNode, AlgebraTreeNode] {

  override def build(from: SpoofaxBaseTreeNode): AlgebraTreeNode = {
    from.name match {
      case "BasicQuery" => {
//        val pathClause = extractClause(from.children.head)
//        val constructClause = extractClause(from.children(1))
        val matchClause = extractClause(from.children(2)).asInstanceOf[MatchClause]

        Query(matchClause)
      }
      case _ => throw QueryParseException(s"Query type ${from.name} unsupported for the moment.")
    }
  }

  /**
    * Builds the subtree of one of the possile clauses in a [[Query]]: path, construct and
    * [[MatchClause]].
    */
  private def extractClause(from: SpoofaxBaseTreeNode): AlgebraOperator = {
    from.name match {
      case "Match" => {
        val fullGraphPatternCondition = from.children.head
        val optionalClause = from.children(1)

        MatchClause(
          extractCondMatchClause(from = fullGraphPatternCondition),
          extractOptionalMatches(from = optionalClause))
      }
      case _ => throw QueryParseException(s"Cannot extract clause from node type ${from.name} ")
    }
  }

  /** Creates a [[algebra.operators.CondMatchClause]] from a given FullGraphPatternCondition. */
  private def extractCondMatchClause(from: SpoofaxBaseTreeNode): CondMatchClause = {
    from.name match {
      case "FullGraphPatternCondition" => {
        val fullGraphPattern = from.children.head
        val where = from.children(1)

        CondMatchClause(
          extractSimpleMatches(fullGraphPattern),
          extractWhere(where))
      }
      case _ =>
        throw QueryParseException(s"Cannot extract CondMatchClause from node type ${from.name}")
    }
  }

  /** Extracts the optional [[CondMatchClause]]s from a given OptionalClause. */
  private def extractOptionalMatches(from: SpoofaxBaseTreeNode): Seq[CondMatchClause] = {
    from.name match {
      case "None" => List.empty
      // Some(OptionalClause(...))
      case "Some" => extractOptionalMatches(from.children.head)
      case "OptionalClause" => {
        // OptionalClause.children = Seq[Optional]
        from.children.map(optional => {
          extractCondMatchClause(from = optional.children.head)
        })
      }
      case _ =>
        throw QueryParseException(s"Cannot extract OptionalClause from node type ${from.name}")
    }
  }

  /** Extracts a [[SimpleMatchClause]] from a given FullGraphPattern node. */
  private def extractSimpleMatches(from: SpoofaxBaseTreeNode): Seq[SimpleMatchClause] = {
    from.name match {
      case "FullGraphPattern" => {
        // FullGraphPattern.children = Seq[BasicGraphPatternLocation]
        from.children.map(basicGraphPatternLocation => {
          val basicGraphPattern = basicGraphPatternLocation.children.head
          val location = basicGraphPatternLocation.children(1)

          SimpleMatchClause(
            extractPattern(basicGraphPattern),
            extractLocation(location))
        })
      }
      case _ =>
        throw QueryParseException(s"Cannot extract SimpleMatchClause from node type ${from.name}")
    }
  }

  /** Extracts the [[Graph]] expression from the Location node. */
  private def extractLocation(from: SpoofaxBaseTreeNode): Graph = {
    from.name match {
      case "None" => new DefaultGraph
      // Some(Location(...)
      case "Some" => extractLocation(from.children.head)
      // Location(...)
      case "Location" => extractLocation(from.children.head)
      case "SpoofaxLeaf" => NamedGraph(from.asInstanceOf[SpoofaxLeaf[String]].value)
      case "SubQuery" => extractLocation(from.children.head)
      case "BasicQuery" => QueryGraph(build(from).asInstanceOf[Query])
    }
  }

  /** Extracts the condition of a [[CondMatchClause]] from the Where node of the lexical tree. */
  private def extractWhere(from: SpoofaxBaseTreeNode): AlgebraExpression = {
    from.name match {
      case "None" => new True
      // Some(Where(...)
      case "Some" => extractWhere(from.children.head)
      case "Where" => extractExpression(from.children.head)
      case _ => throw QueryParseException(s"Cannot extract Expression from node type ${from.name}")
    }
  }

  /** Builds an [[AlgebraExpression]] from any possible lexical node of an expression. */
  private def extractExpression(from: SpoofaxBaseTreeNode): AlgebraExpression = {
    from.name match {
      /* Unary expressions. */
      case "Not" => Not(extractExpression(from.children.head))
      case "UMin" => Minus(extractExpression(from.children.head))

      /* TODO: Binary expressions. */

      /* TODO: Predicate expressions. */

      /* Other algebra expressions. */
      case "VarRef" => Reference(from.children.head.asInstanceOf[SpoofaxLeaf[String]].value)
      case "Integer" =>
        Literal[Integer](from.children.head.asInstanceOf[SpoofaxLeaf[String]].value.toInt)
      case "True" => True()
      case "False" => False()
      case "String" => Literal[String](from.children.head.asInstanceOf[SpoofaxLeaf[String]].value)
      case "BasicGraphPattern" =>
        // TODO: The Query operator should also take an empty ConstructClause in this case.
        Exists(
          Query(
            MatchClause(
              CondMatchClause(
                List(SimpleMatchClause(extractPattern(from), new DefaultGraph)),
                /*where = */ True()),
              List.empty[CondMatchClause]
            )
          )
        )

      /* Default case. */
      case _ => throw QueryParseException(s"Unsupported expression ${from.name}")
    }
  }

  /**
    * Builds the topology of the graph pattern under the BasicGraphPattern node.
    *
    * @see [[GraphPattern]]
    */
  private def extractPattern(from: SpoofaxBaseTreeNode): GraphPattern = {
    from.name match {
      case "BasicGraphPattern" =>
        // TODO: The empty Vertex should not be expressed with a null.
        GraphPattern(extractPatternTopology(from.children, null))
      case _ =>
        throw QueryParseException(s"Cannot extract GraphPattern from node type ${from.name}")
    }
  }

  /** Helper method to extract the topology from the children of a BasicGraphPattern. */
  private def extractPatternTopology(from: Seq[SpoofaxBaseTreeNode],
                                     prevRef: SingleEndpointConn): Seq[Connection] = {
    from match {
      // Vertex
      case Seq(vertex) if vertex.name == "Vertex" => Seq(extractVertex(vertex))

      // Vertex - EdgeVertexMatchPattern - ...
      case Seq(vertex, connections@_*) if vertex.name == "Vertex" =>
        extractPatternTopology(
          /*from =*/    connections,
          /*prevRef =*/ extractVertex(vertex))

      // ... - EdgeVertexMatchPattern - ...
      case Seq(connection, connections@_*) if connection.name == "EdgeVertexMatchPattern" => {
        val extractedConnection: DoubleEndpointConn = extractConnection(connection, prevRef)
        extractedConnection +:
          extractPatternTopology(connections, extractedConnection.getRightEndpoint)
      }

      case _ => Seq()
    }
  }

  /** Creates an algebraic [[Vertex]] from a Vertex lexical node. */
  private def extractVertex(from: SpoofaxBaseTreeNode): SingleEndpointConn = {
    // Vertex -> Some -> VarDef -> SpoofaxLeaf
    val refName = from.children.head /*Some*/
      .children.head /*VarDef*/
      .children.head.asInstanceOf[SpoofaxLeaf[String]] /*SpoofaxLeaf*/
    Vertex(Reference(refName.value), extractMatchPattern(from.children(1)))
  }

  /** Helper function to extract either a [[Path]] or an [[Edge]]. */
  private def extractConnection(from: SpoofaxBaseTreeNode, // EdgeVertexMatchPattern
                                prevRef: SingleEndpointConn): DoubleEndpointConn = {
    val connElement = from.children.head /*connType*/
      .children.head /*Some*/
      .children.head /*Edge or Path*/

    connElement.name match {
      case "Edge" => extractEdge(from, prevRef)
      case "Path" => extractPath(from, prevRef)
    }
  }

  /** Creates an algebraic [[Edge]] from an Edge lexical node. */
  private def extractEdge(from: SpoofaxBaseTreeNode,
                          prevRef: SingleEndpointConn): Edge = {
    val connElement = from.children.head /*connType*/
      .children.head /*Some*/
      .children.head /*Edge*/
    val refName = connElement.children.head /*EdgeMatchPattern*/
      .children.head /*Some*/
      .children.head /*VarDef*/
      .children.head.asInstanceOf[SpoofaxLeaf[String]] /*SpoofaxLeaf*/
    val connName = Reference(refName.value)

    val connType = from.children.head.name match {
      case "InConn" => InConn()
      case "OutConn" => OutConn()
      case "UndirectedEdge" => UndirectedConn()
      case "InOutEdge" => InOutConn()
    }

    val expr = extractMatchPattern(connElement.children.head.children(1))

    val rightEndpoint = extractVertex(from.children(1))

    Edge(connName, leftEndpoint = prevRef, rightEndpoint, connType, expr)
  }

  /** Creates an algebraic [[Path]] from a Path lexical node. */
  private def extractPath(from: SpoofaxBaseTreeNode,
                          prevRef: SingleEndpointConn): Path = {
    val connElement = from.children.head /*connType*/
      .children.head /*Some*/
      .children.head /*Path*/

    // If this is an objectified path, the lexical tree will also contain a most redundant node,
    // SpoofaxLeaf["@"]. We remove this node to avoid checking the path type for each Path element.
    val pathType = connElement.children.head /*Virtual/Objectified*/
    if (pathType.name == "Objectified")
      pathType.children = List(pathType.children.head) ++ pathType.children.drop(2)

    val refName = pathType.children(1) /*Some or None*/
    val connName = refName.name match {
      case "None" => Option.empty
      case "Some" =>
        Option(Reference(refName.children.head /*VarDef */
          .children.head /*SpoofaxLeaf */
          .asInstanceOf[SpoofaxLeaf[String]].value))
    }

    val connType = from.children.head.name match {
      case "InConn" => InConn()
      case "OutConn" => OutConn()
      case "UndirectedEdge" => UndirectedConn()
      case "InOutEdge" => InOutConn()
    }

    val expr = connElement.children.head.name match {
      case "Virtual" => ObjectPattern(True(), True()) // A Virtual path has no ObjectMatchPattern
      case "Objectified" => extractMatchPattern(connElement.children.head.children(3))
    }

    val rightEndpoint = extractVertex(from.children(1))

    val isObj = connElement.name == "Path" && connElement.children.head.name == "Objectified"

    val quantifierElement = connElement.children.head /* Virtual/Objectified */
        .children.head /* Some/None */
      val quantifier = quantifierElement.name match {
      case "None" => Option.empty
      case "Some" => quantifierElement.children.head.name match {
        case "Shortest" => Option(Shortest(qty = 1, isDistinct = false))
        case "XShortest" =>
          Option(Shortest(qty = quantifierElement.children.head.children.head
            .asInstanceOf[SpoofaxLeaf[String]].value.toInt,
            isDistinct = false))
        case "XDistinctShortest" =>
          Option(Shortest(qty = quantifierElement.children.head.children.head
            .asInstanceOf[SpoofaxLeaf[String]].value.toInt,
            isDistinct = true))
        case "AllPaths" => Option(new AllPaths)
      }
    }

    val costVarDefElement = connElement.children.head /* Virtual/Objectified */
        .children.last /* None or Some */
    val costVarDef = costVarDefElement.name match {
      case "None" => Option.empty
      case "Some" => {
        val costVarElement = costVarDefElement.children.head
        val varDefElement = costVarElement.children.head
        Option(Literal[String](varDefElement.children.head.asInstanceOf[SpoofaxLeaf[String]].value))
      }
    }

    Path(connName, leftEndpoint = prevRef, rightEndpoint, connType,
      expr, quantifier, costVarDef, isObj)
  }

  /**
    * Extracts the matching pattern of an entity from an ObjectMatchPattern lexical node.
    *
    * @see [[GraphPattern]]
    */
  private def extractMatchPattern(from: SpoofaxBaseTreeNode): ObjectPattern = {
    from.name match {
      case "ObjectMatchPattern" =>
        ObjectPattern(extractLabels(from.children.head), extractProps(from.children(1)))
      case _ =>
        throw QueryParseException(s"Cannot extract ObjectMatchPattern from node type ${from.name}")
    }
  }

  private def extractLabels(from: SpoofaxBaseTreeNode): AlgebraExpression = {
    from.name match {
      case "None" => True()
      case "Some" => WithLabels(extractLabels(from.children.head))
      case "ConjunctLabels" => extractDisjunctLabels(from.children)
      case _ => throw QueryParseException(s"Cannot extract labels from node type ${from.name}")
    }
  }

  /**
    * @see [[GraphPattern]]
    */
  private def extractDisjunctLabels(dls: Seq[SpoofaxBaseTreeNode]): AlgebraExpression = {
    // dls = Seq[DisjunctLabels]
    dls match {
      // dl = DisjunctLabels
      case Seq(dl, other@_*) =>
        And(
          HasLabel(
            dl.children.map(
              label => Label(label.children.head.asInstanceOf[SpoofaxLeaf[String]].value))),
          extractDisjunctLabels(other))
      case _ => new True
    }
  }

  private def extractProps(from: SpoofaxBaseTreeNode): AlgebraExpression = {
    from.name match {
      case "None" => new True
      case "Some" => extractProps(from.children.head)
      case "Props" => WithProps(extractConjunctProps(from.children))
      case _ => throw QueryParseException(s"Cannot extract properties from node type ${from.name}")
    }
  }

  /**
    * @see [[GraphPattern]]
    */
  private def extractConjunctProps(cps: Seq[SpoofaxBaseTreeNode]): AlgebraExpression = {
    cps match {
      // { and } => move on to other
      case Seq(prop, other@_*) if prop.name == "SpoofaxLeaf" => extractConjunctProps(other)
      case Seq(prop, other@_*) if prop.name == "Prop" =>
        And(
          Eq(
            PropertyKey(prop.children.head.asInstanceOf[SpoofaxLeaf[String]].value),
            extractExpression(prop.children(1))),
          extractConjunctProps(other))
      case Seq() => new True
    }
  }
}
