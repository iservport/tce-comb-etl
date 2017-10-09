package com.iservport.tce.actor

import akka.actor.{Actor, ActorLogging, Props}

/**
  * Adapta o domínio do TCE ao domínio da aplicação.
  */
class AdapterActor extends Actor with ActorLogging {

  lazy val collectionActor = context.actorOf(Props(new CollectionActor()) , "_collection")

  override def receive: Receive = {
    case ("Combustivel", attributes: Map[String, String]) =>
      collectionActor ! CityData(attributes)
      collectionActor ! EntityData(attributes)
      collectionActor ! Vehicle(attributes)
    case ("QuantidadeConsumoCombustivelVeiculoEq", attributes: Map[String, String]) =>
      collectionActor ! Quantity(attributes)
    case ("QuantitativaCombustivel", attributes: Map[String, String]) =>
      collectionActor ! Price(attributes)
    case other => println(other)
  }

}
