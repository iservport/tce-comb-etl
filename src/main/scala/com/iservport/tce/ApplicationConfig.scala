package com.iservport.tce

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.ReadConfig
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.{MongoConnection, MongoDriver}

import scala.concurrent.{ExecutionContext, Future}

object ApplicationConfig {

  val ibgeCsv = getClass.getResource("/ibge.csv").getPath

  val config = ConfigFactory.load()
  val hosts = config.getStringList("mongodb.servers")
  val database = config.getString("mongodb.database")
  val mongoUri = s"mongodb://${hosts.get(0)}/$database"

  val driver = new MongoDriver
  implicit val connection: MongoConnection = driver.connection(hosts.get(0)).get

  def fromConnection(collection: String)(implicit ec: ExecutionContext, connection: MongoConnection): Future[BSONCollection] =
    connection.database(database).map(_.collection(collection))

  /**
    * Obtém a coleção como DataFrame.
    */
  def toDataFrame(collName: String)(implicit session: SparkSession) = {
    val config = ReadConfig(Map("uri"  -> s"$mongoUri.$collName?readPreference=primaryPreferred"))
    MongoSpark.load(session, config).persist
  }


}
