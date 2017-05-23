package com.dsl.graph.entities.person

import java.util.UUID

import com.dsl.graph.entities.Common.ID

/**
  * Actor Id of entity person
  * Created by shishir on 5/18/17.
  */
case class PersonId(override val resourceId: UUID) extends ID(resourceId)

object PersonId {
  def create(resourceId: UUID): PersonId = PersonId(UUID.randomUUID())
}
