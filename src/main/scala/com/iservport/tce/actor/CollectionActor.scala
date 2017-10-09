package com.iservport.tce.actor

import akka.actor.{Actor, ActorLogging}

/**
  * Atualiza a base de dados.
  */
class CollectionActor extends Actor with ActorLogging {

  implicit val executor = context.dispatcher

  import com.iservport.tce.ApplicationConfig._

  val cityColl     = fromConnection("cityData")
  val entityColl   = fromConnection("entityData")
  val vehicleColl  = fromConnection("vehicle")
  val quantityColl = fromConnection("quantity")
  val priceColl    = fromConnection("price")

  override def receive: Receive = {
    case d: CityData    => cityColl.flatMap(_.insert(d.doc))
    case d: CityGeo     => cityColl.flatMap(_.insert(d.doc))
    case d: EntityData  => entityColl.flatMap(_.insert(d.doc))
    case d: Vehicle     => vehicleColl.flatMap(_.insert(d.doc))
    case d: Quantity    => quantityColl.flatMap(_.insert(d.doc))
    case d: Price       => priceColl.flatMap(_.insert(d.doc))
  }

}
