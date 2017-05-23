package com.dsl.graph.service

import java.util.UUID

import akka.actor.ActorSystem
import com.dsl.graph.entities.Common.{ID, Metadata, Resource}
import com.dsl.graph.entities.business.{Business, BusinessId, BusinessMetadata}
import com.dsl.graph.manager.ResourceManagerResolver

/**
  * Anything we need to add to a resource can be done here
  * Created by shishir on 5/19/17.
  */
class BusinessService(val system: ActorSystem) extends ResourceService {

  override def createEmptyResource(resourceId: UUID, metadata: Metadata): Business = {
    val businessMetadata = metadata.asInstanceOf[BusinessMetadata]
    Business(BusinessId(resourceId), metadata = businessMetadata)
  }

  override def getActorProps(implicit actorSystem: ActorSystem) = {
    ResourceManagerResolver.props
  }

  override def updateResource(resource: Resource, newMetadata: Metadata): Business = {
    val business = resource.asInstanceOf[Business]
    val businessMetadata = newMetadata.asInstanceOf[BusinessMetadata]
    business.copy(metadata = businessMetadata)
  }

  override def getAllListeners(resource: Resource): List[ID] = {
    val business = resource.asInstanceOf[Business]
    (business.publishTopics ++ business.subscriptionTopics).flatMap(_._2).toList
  }

}
