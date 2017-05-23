package com.dsl.graph.service

import java.util.UUID

import akka.actor.{ActorContext, ActorSystem, Props}
import com.dsl.graph.entities.Common.{ID, Metadata, Resource}
import org.slf4j.LoggerFactory

/**
  * Created by shishir on 5/18/17.
  */
trait ResourceService {

  val logger = LoggerFactory.getLogger(this.getClass)

  def updateResource(resource: Resource, metadata: Metadata): Resource

  def createEmptyResource(resourceId: UUID, metadata: Metadata): Resource

  def getActorProps(implicit actorSystem: ActorSystem): Props

  def getAllListeners(resource: Resource): List[ID]

}
