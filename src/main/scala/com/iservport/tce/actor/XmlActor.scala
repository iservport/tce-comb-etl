package com.iservport.tce.actor

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.stream.ActorMaterializer
import akka.stream.alpakka.xml.{ParseEvent, StartElement}
import akka.stream.alpakka.xml.scaladsl.XmlParsing
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.util.ByteString

/**
  * Converte o xml em um mapa de atributos.
  */
class XmlActor extends Actor with ActorLogging {

  implicit val executor = context.dispatcher
  implicit val mat = ActorMaterializer()

  lazy val adapterActor = context.actorOf(Props(new AdapterActor()) , "_adapter")

  override def receive: Receive = {
    case content: String => Source.single(content).runWith(parse).map(elem => self ! elem)
    case events: Vector[ParseEvent] => events.foreach(e => self ! e)
    case StartElement(elementName, attributes: Map[String, String]) if valid(elementName) =>
      adapterActor ! (elementName, attributes)
    case other => print(".")
  }

  val parse = Flow[String].map(ByteString(_)).via(XmlParsing.parser).toMat(Sink.seq)(Keep.right)

  def valid(elementName: String) =
    Array("Combustivel", "QuantitativaCombustivel", "VeiculoEquipamento").contains(elementName)

}
