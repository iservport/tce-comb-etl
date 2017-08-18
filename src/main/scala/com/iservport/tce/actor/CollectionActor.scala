package com.iservport.tce.actor

import akka.actor.{Actor, ActorLogging}
import com.typesafe.config.ConfigFactory
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.{MongoConnection, MongoDriver}

import scala.concurrent.Future

/**
  * Created by mauriciofernandesdecastro on 29/05/17.
  */
class CollectionActor extends Actor with ActorLogging {

  implicit val executor = context.dispatcher

//  val config = ConfigFactory.load()
//  val database = config.getString("mongodb.database") // TODO not used

  val driver = new MongoDriver
  implicit val connection: MongoConnection = driver.connection("localhost:27017").get

  def fromConnection(collection: String)(implicit connection: MongoConnection): Future[BSONCollection] =
    connection.database("InovaTCE").map(_.collection(collection))

//  val db = connection(database)

  val cityColl    = fromConnection("cityData")
  val entityColl  = fromConnection("entityData")
  val vehicleColl = fromConnection("vehicle")
  val usageColl   = fromConnection("usage")
  val quantityColl   = fromConnection("quantity")
//  val vehicleDataColl   = db.getCollection("vehicleData")

  override def receive: Receive = {
    case d: CityData    => cityColl.flatMap(_.insert(d.doc))
    case d: CityGeo     => cityColl.flatMap(_.insert(d.doc))
    case d: EntityData  => entityColl.flatMap(_.insert(d.doc))
    case d: Vehicle     => vehicleColl.flatMap(_.insert(d.doc))
    case d: Usage       => usageColl.flatMap(_.insert(d.doc))
    case d: Quantity    => quantityColl.flatMap(_.insert(d.doc))
  }

}
