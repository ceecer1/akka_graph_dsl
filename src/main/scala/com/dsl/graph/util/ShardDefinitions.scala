package com.dsl.graph.util

import akka.actor.ActorSystem
import com.dsl.graph.manager.ResourceManager

/**
  * Created by shishir on 5/18/17.
  */
class ShardDefinitions(implicit val actorSystem: ActorSystem) {


  val personManager = new ShardableResource(10, "personManager", ResourceManager.personManager, actorSystem)
  val businessManager = new ShardableResource(10, "businessManager", ResourceManager.businessManager, actorSystem)


}

object ShardDefinitions {

  private var definitions: Option[ShardDefinitions] = None

  def shards(implicit actorSystem: ActorSystem): ShardDefinitions = {
    definitions match {
      case Some(value) =>
        value
      case None =>
        val localDefinitions = new ShardDefinitions()
        definitions = Some(localDefinitions)
        localDefinitions
    }
  }

  def apply(implicit actorSystem: ActorSystem) = new ShardDefinitions()
}
