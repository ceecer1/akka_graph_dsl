package com.dsl.graph.util

import com.typesafe.config.ConfigFactory

/**
  * Created by shishir on 5/19/17.
  */
object EdgesConfig {

  private val edgeConfig = ConfigFactory.load().getString("edges")
  private val edgePairs = edgeConfig.split(",").toVector
  private val edgeTuples = edgePairs.map {
    pair => pair.split("#") match {
      case Array(x, y) => Tuple2(x, y)
    }
  }

  def getRelationPath(edgePath: String): String = {
    val relation = edgeTuples.flatMap {
      case (x, y) if x == edgePath => Vector(x, y)
      case (x, y) if y == edgePath => Vector(y, x)
      case _ => Vector()
    }

    edgePath match {
      case head if edgePath == relation.head => relation.tail.head
      case tail if edgePath == relation.tail.head => relation.head
      case _ => edgePath
    }
  }

}
