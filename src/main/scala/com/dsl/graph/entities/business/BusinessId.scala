package com.dsl.graph.entities.business

import java.util.UUID

import com.dsl.graph.entities.Common.ID

/**
  * Actor id of entity Business
  * Created by shishir on 5/18/17.
  */
case class BusinessId(override val resourceId: UUID) extends ID(resourceId)

object BusinessId {
  def create(resourceId: UUID): BusinessId = BusinessId(UUID.randomUUID())
}


