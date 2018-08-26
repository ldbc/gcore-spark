package parser.trees

import algebra.expressions._
import algebra.operators.{CondMatchClause, MatchClause, SimpleMatchClause}
import algebra.types._
import parser.exceptions.QueryParseException
import parser.trees.ExpressionTreeBuilder._
import parser.trees.QueryTreeBuilder.extractQueryClause
import parser.utils.VarBinder

object MatchTreeBuilder {

  /** Creates a [[MatchClause]] from a given Match node. */
  def extractMatchClause(from: SpoofaxBaseTreeNode): MatchClause = {
    val fullGraphPatternCondition = from.children.head
    val optionalClause = from.children.last

    MatchClause(
      nonOptMatches = extractCondMatchClause(fullGraphPatternCondition),
      optMatches = extractOptionalMatches(optionalClause))
  }

  /** Creates a [[CondMatchClause]] from a given FullGraphPatternCondition. */
  private def extractCondMatchClause(from: SpoofaxBaseTreeNode): CondMatchClause = {
    from.name match {
      case "FullGraphPatternCondition" =>
        val fullGraphPattern = from.children.head
        val where = from.children(1)

        CondMatchClause(
          extractSimpleMatches(fullGraphPattern),
          extractWhere(where))

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
      case "OptionalClause" =>
        // OptionalClause.children = Seq[Optional]
        from.children.map(optional => {
          extractCondMatchClause(from = optional.children.head)
        })
      case _ =>
        throw QueryParseException(s"Cannot extract OptionalClause from node type ${from.name}")
    }
  }

  /**
    * Builds the topology of the graph pattern under the BasicGraphPattern node.
    *
    * @see [[GraphPattern]]
    */
  def extractGraphPattern(from: SpoofaxBaseTreeNode): GraphPattern = {
    from.name match {
      case "BasicGraphPattern" =>
        GraphPattern(extractPatternTopology(from.children, None))
      case _ =>
        throw QueryParseException(s"Cannot extract GraphPattern from node type ${from.name}")
    }
  }

  /** Extracts a [[SimpleMatchClause]] from a given FullGraphPattern node. */
  private def extractSimpleMatches(from: SpoofaxBaseTreeNode): Seq[SimpleMatchClause] = {
    from.name match {
      case "FullGraphPattern" =>
        // FullGraphPattern.children = Seq[BasicGraphPatternLocation]
        from.children.map(basicGraphPatternLocation => {
          val basicGraphPattern = basicGraphPatternLocation.children.head
          val location = basicGraphPatternLocation.children(1)

          SimpleMatchClause(
            extractGraphPattern(basicGraphPattern),
            extractLocation(location))
        })
      case _ =>
        throw QueryParseException(s"Cannot extract SimpleMatchClause from node type ${from.name}")
    }
  }

  /** Extracts the [[Graph]] expression from the Location node. */
  private def extractLocation(from: SpoofaxBaseTreeNode): Graph = {
    from.name match {
      case "None" => DefaultGraph
      // Some(Location(...)
      case "Some" => extractLocation(from.children.head)
      // Location(...)
      case "Location" => extractLocation(from.children.head)
      case "SpoofaxLeaf" => NamedGraph(from.asInstanceOf[SpoofaxLeaf[String]].value)
      case "SubQuery" => extractLocation(from.children.head)
      case "BasicQuery" => QueryGraph(extractQueryClause(from))
    }
  }

  /**
    * Extracts the condition of a [[CondMatchClause]] from the Where node of the lexical tree. */
  private def extractWhere(from: SpoofaxBaseTreeNode): AlgebraExpression = {
    from.name match {
      case "None" => True
      // Some(Where(...)) for match or Some(ConstructCondition(...)) for construct
      case "Some" => extractWhere(from.children.head)
      case "Where" => extractExpression(from.children.head)
      case "ConstructCondition" => extractExpression(from.children.head)
      case _ => throw QueryParseException(s"Cannot extract Expression from node type ${from.name}")
    }
  }

  /** Helper method to extract the topology from the children of a BasicGraphPattern. */
  private def extractPatternTopology(from: Seq[SpoofaxBaseTreeNode],
                                     prevRef: Option[SingleEndpointConn]): Seq[Connection] = {
    from match {
      // Vertex
      case Seq(vertex) if vertex.name == "Vertex" => Seq(extractVertexMatch(vertex))

      // Vertex - EdgeVertexMatchPattern - ...
      case Seq(vertex, connections@_*) if vertex.name == "Vertex" =>
        extractPatternTopology(
          /*from =*/    connections,
          /*prevRef =*/ Some(extractVertexMatch(vertex)))

      // ... - EdgeVertexMatchPattern - ...
      case Seq(connection, connections@_*) if connection.name == "EdgeVertexMatchPattern" =>
        val extractedConnection: DoubleEndpointConn = extractConnection(connection, prevRef.get)
        extractedConnection +:
          extractPatternTopology(connections, Some(extractedConnection.getRightEndpoint))

      case _ => Seq.empty
    }
  }

  /** Creates an algebraic [[Vertex]] from a Vertex lexical node. */
  private def extractVertexMatch(from: SpoofaxBaseTreeNode): SingleEndpointConn = {
    // Vertex -> Some -> VarDef -> SpoofaxLeaf
    val refName = from.children.head /*Some*/
      .children.head /*VarDef*/
      .children.head.asInstanceOf[SpoofaxLeaf[String]] /*SpoofaxLeaf*/
    Vertex(Reference(refName.value), extractObjectPattern(from.children(1)))
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
      case "InConn" => InConn
      case "OutConn" => OutConn
      case "UndirectedEdge" => UndirectedConn
      case "InOutEdge" => InOutConn
    }

    val expr = extractObjectPattern(connElement.children.head.children(1))

    val rightEndpoint = extractVertexMatch(from.children(1))

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
      pathType.children = List(pathType.children.head) ++ pathType.children.drop(2) // drop first 2

    val refName = pathType.children(1) /*Some or None*/
    val (connName, isReachableTest) = refName.name match {
      case "None" => (Reference(VarBinder.createVar("p")), true)
      case "Some" =>
        (Reference(refName.children.head /*VarDef */
          .children.head /*SpoofaxLeaf */
          .asInstanceOf[SpoofaxLeaf[String]].value),
          false)
    }

    val connType = from.children.head.name match {
      case "InConn" => InConn
      case "OutConn" => OutConn
      case "UndirectedEdge" => UndirectedConn
      case "InOutEdge" => InOutConn
    }

    val expr = connElement.children.head.name match {
      case "Virtual" => ObjectPattern(True, True) // A Virtual path has no ObjectMatchPattern
      case "Objectified" => extractObjectPattern(connElement.children.head.children(3))
    }

    val rightEndpoint = extractVertexMatch(from.children(1))

    val isObj = connElement.name == "Path" && connElement.children.head.name == "Objectified"

    val quantifierElement = connElement.children.head /* Virtual/Objectified */
      .children.head /* Some/None */
    val quantifier = quantifierElement.name match {
      // Default for objectified is AllPaths, default for virtual is shortest.
      case "None" => if (isObj) AllPaths else Shortest(qty = 1, isDistinct = false)
      case "Some" => quantifierElement.children.head.name match {
        case "Shortest" => Shortest(qty = 1, isDistinct = false)
        case "XShortest" =>
          Shortest(
            qty =
              quantifierElement.children.head
                .children.head.asInstanceOf[SpoofaxLeaf[String]]
                .value.toInt,
            isDistinct = false)
        case "XDistinctShortest" =>
          Shortest(
            qty =
              quantifierElement.children.head
                .children.head.asInstanceOf[SpoofaxLeaf[String]]
                .value.toInt,
            isDistinct = true)
        case "AllPaths" => AllPaths
      }
    }

    val costVarDefElement =
      connElement.children.head /* Virtual/Objectified */
        .children.last /* None or Some */
    val costVarDef = costVarDefElement.name match {
      case "None" => None
      case "Some" =>
        val costVarElement = costVarDefElement.children.head
        val varDefElement = costVarElement.children.head
        Some(Reference(varDefElement.children.head.asInstanceOf[SpoofaxLeaf[String]].value))
    }

    val pathExpressionElement =
      connElement.children.head /* Virtual/Objectified */
        .children(2) /* None or Some */
    val pathExpression = pathExpressionElement.name match {
      case "None" => None
      case "Some" =>
        Some(
          extractPathExpression(
            pathExpressionElement.children.head // PathExpression
              .children(1))) // children.head is SpoofaxLeaf [<]
    }

    Path(connName, isReachableTest, leftEndpoint = prevRef, rightEndpoint, connType,
      expr, quantifier, costVarDef, isObj, pathExpression)
  }

  private def extractPathExpression(from: SpoofaxBaseTreeNode): PathExpression = {
    from.name match {
      // There is a typo in the node name in the language reference, "marco" instead of "macro".
      case "MarcoNameRef" =>
        MacroNameReference(Reference(from.children.head.asInstanceOf[SpoofaxLeaf[String]].value))
      case "KleeneStar" =>
        val disjunctLabels =
          DisjunctLabels(
            from.children.head.children.map(
              label => Label(label.children.head.asInstanceOf[SpoofaxLeaf[String]].value)))
        val (lowerBound, upperBound) = from.children.last.name match {
          case "None" => (0, Int.MaxValue)
          case "Some" =>
            val boundsElement = from.children.last.children.head
            boundsElement.name match {
              case "Lower" =>
                val lower = boundsElement.children(1).asInstanceOf[SpoofaxLeaf[String]].value.toInt
                val upper = Int.MaxValue
                (lower, upper)
              case "Upper" =>
                val lower = 0
                val upper = boundsElement.children(2).asInstanceOf[SpoofaxLeaf[String]].value.toInt
                (lower, upper)
              case "LowerUpper" =>
                val lower = boundsElement.children(1).asInstanceOf[SpoofaxLeaf[String]].value.toInt
                val upper = boundsElement.children(3).asInstanceOf[SpoofaxLeaf[String]].value.toInt
                (lower, upper)
            }
        }

        KleeneStar(disjunctLabels, lowerBound, upperBound)
      case "Concatenation" =>
        KleeneConcatenation(
          lhs = extractPathExpression(from.children.head),
          rhs = extractPathExpression(from.children.last))
      case "Union" =>
        KleeneUnion(
          lhs = extractPathExpression(from.children.head),
          rhs = extractPathExpression(from.children.last))
      case other =>
        throw QueryParseException(s"Cannot extract PathExpression from node type $other")
    }
  }

  /**
    * Extracts the matching pattern of an entity from an ObjectMatchPattern lexical node.
    *
    * @see [[GraphPattern]]
    */
  private def extractObjectPattern(from: SpoofaxBaseTreeNode): ObjectPattern = {
    from.name match {
      case "ObjectMatchPattern" =>
        ObjectPattern(extractLabels(from.children.head), extractProps(from.children(1)))
      case _ =>
        throw QueryParseException(s"Cannot extract ObjectMatchPattern from node type ${from.name}")
    }
  }

  private def extractLabels(from: SpoofaxBaseTreeNode): AlgebraExpression = {
    from.name match {
      case "None" => True
      case "Some" => ConjunctLabels(extractLabels(from.children.head))
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
          DisjunctLabels(
            dl.children.map(
              label => Label(label.children.head.asInstanceOf[SpoofaxLeaf[String]].value))),
          extractDisjunctLabels(other))
      case _ => True
    }
  }

  private def extractProps(from: SpoofaxBaseTreeNode): AlgebraExpression = {
    from.name match {
      case "None" => True
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
      case Seq() => True
    }
  }
}
