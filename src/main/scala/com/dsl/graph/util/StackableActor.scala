package com.dsl.graph.util

import akka.actor.{Actor, ActorLogging}
import akka.persistence.PersistentActor
import com.dsl.graph.entities.Common.{ID, Request}
import com.dsl.graph.manager.ResourceManager

import scala.concurrent.duration._

/**
  * Created by shishir on 5/18/17.
  */
trait StackableActor extends PersistentActor {
  /*
  this is passivation timeout
   */
  context.setReceiveTimeout(120.seconds)
  var wrappedReceive: Receive = {
    case x => unhandled(x)
  }

  def wrappedBecome(r: Receive) = {
    wrappedReceive = r
  }

  def receiveCommand: Receive = {
    case x => if (wrappedReceive.isDefinedAt(x)) wrappedReceive(x) else unhandled(x)
  }
}

/**
  * Handles the publishing and subscribing of messages
  */
trait PubSubManager extends StackableActor with ActorLogging {
  self: ResourceManager =>
  override def receive: Actor.Receive = {

    case AddPublishTopic(edge, id) =>
      log.debug(s"Adding publish topic $id to resource $persistentResource")
      persistentResource = persistentResource.addPublishTopic(edge, id)
      saveSnapshot(persistentResource)

    case RemovePublishTopic(edge, id) =>
      log.debug(s"Removing publish topic $id from resource $persistentResource")
      persistentResource = persistentResource.removePublishTopic(edge, id)
      saveSnapshot(persistentResource)

    case AddSubscriptionTopic(edge, id) =>
      log.info(s"Adding subscription topic $id to resource $persistentResource")
      persistentResource = persistentResource.addSubscriptionTopic(edge, id)
      saveSnapshot(persistentResource)

    case RemoveSubscriptionTopic(edge, id) =>
      log.debug(s"Removing topic $id from resource $persistentResource")
      persistentResource = persistentResource.removeSubscriptionTopic(edge, id)
      saveSnapshot(persistentResource)

    case x => {
      super.receive(x)
    }
  }
}

case class AddPublishTopic(edge: String, id: ID) extends Request

case class RemovePublishTopic(edge: String, id: ID) extends Request

case class AddSubscriptionTopic(edge: String, id: ID) extends Request

case class RemoveSubscriptionTopic(edge: String, id: ID) extends Request
