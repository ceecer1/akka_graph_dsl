package com.dsl.graph.directors

import akka.actor.{ActorRef, ActorSystem, Props}

/**
  * Created by shishir on 5/20/17.
  */
object EntityDirectorFactory {

  var entityDirector: ActorRef = null

  def createEntityDirector(actorSystem: ActorSystem) = entityDirector = actorSystem.actorOf(Props[EntityDirector], "EntityDirector")

}
