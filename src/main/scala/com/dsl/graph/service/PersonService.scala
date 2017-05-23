package com.dsl.graph.service

import java.util.UUID

import akka.actor.ActorSystem
import com.dsl.graph.entities.Common.{ID, Metadata, Resource}
import com.dsl.graph.entities.person.{Person, PersonId, PersonMetadata}
import com.dsl.graph.manager.ResourceManagerResolver

/**
  * * Anything we need to add to a resource can be done here
  * Created by shishir on 5/19/17.
  */
class PersonService(val system: ActorSystem) extends ResourceService {

  override def createEmptyResource(resourceId: UUID, metadata: Metadata): Person = {
    val personMetadata = metadata.asInstanceOf[PersonMetadata]
    Person(PersonId(resourceId), metadata = personMetadata)
  }

  override def getActorProps(implicit actorSystem: ActorSystem) = {
    ResourceManagerResolver.props
  }

  override def updateResource(resource: Resource, newMetadata: Metadata): Person = {
    val person = resource.asInstanceOf[Person]
    val personMetadata = newMetadata.asInstanceOf[PersonMetadata]
    person.copy(metadata = personMetadata)
  }

  override def getAllListeners(resource: Resource): List[ID] = {
    val person = resource.asInstanceOf[Person]
    (person.publishTopics ++ person.subscriptionTopics).flatMap(_._2).toSet.toList
  }

}
