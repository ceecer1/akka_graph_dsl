package com.dsl.graph.entities.person

import com.dsl.graph.entities.Common.{ID, Resource}

/**
  * Created by shishir on 5/18/17.
  */
case class Person(id: PersonId,
                 metadata: PersonMetadata,
                 subscriptionTopics: Map[String, Set[ID]] = Map.empty,
                 publishTopics: Map[String, Set[ID]] = Map.empty) extends Resource {

  /**
    * Subscription topics holds the incoming relationships
    * e.g if a person x adds y as friend, y gets the subscription message such that it also stores the relation info
    * @param edge Relationship name, e.g. friendOf, valueOf, relationOf
    * @param id target id to add
    * @return
    */
  override def addSubscriptionTopic(edge: String, id: ID): Resource = {
    val newTopics = subscriptionTopics.contains(edge) match {
      case true   => Map(edge -> (subscriptionTopics.get(edge).getOrElse(Set()) ++ Set(id)))
      case false  => subscriptionTopics ++ Map(edge -> Set(id))
    }

    this.copy(subscriptionTopics = newTopics)
  }


  //topic edge has to be in- to be unsubscribed
  //FIXME for Set difference with empty Sets
  /**
    * Removes the topic when remove subscription topic message is got
    * topic edge has to be in- to be unsubscribed
    * @param edge Relationship name, e.g. friendOf, valueOf, relationOf
    * @param id target id to remove
    * @return
    */
  override def removeSubscriptionTopic(edge: String, id: ID): Resource = {
    val newTopics = Map(edge -> (subscriptionTopics.get(edge).getOrElse(Set()) -- Set(id)))
    this.copy(subscriptionTopics = newTopics)
  }

  /**
    * Adds info to this resource's publish topics
    * @param edge Relationship name, e.g. friendOf, valueOf, relationOf
    * @param id target id to add
    * @return
    */
  override def addPublishTopic(edge: String, id: ID): Resource = {
    val newTopics = publishTopics.contains(edge) match {
      case true   => Map(edge -> (publishTopics.get(edge).getOrElse(Set()) ++ Set(id)))
      case false  => publishTopics ++ Map(edge -> Set(id))
    }
    this.copy(publishTopics = newTopics)
  }

  /**
    * Removes info from this resource's publish topics
    * @param edge Relationship name, e.g. friendOf, valueOf, relationOf
    * @param id target id to remove
    * @return
    */
  override def removePublishTopic(edge: String, id: ID): Resource = {
    val newTopics = Map(edge -> (publishTopics.get(edge).getOrElse(Set()) -- Set(id)))
    this.copy(publishTopics = newTopics)
  }

}
