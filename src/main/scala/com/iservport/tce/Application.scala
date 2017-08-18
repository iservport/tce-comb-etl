package com.iservport.tce

import java.util.zip.ZipInputStream

import akka.actor.{ActorSystem, Props}
import akka.stream.ActorMaterializer
import akka.util.Timeout
import com.iservport.tce.actor.XmlActor
import org.apache.spark.sql.SparkSession

import scala.concurrent.duration._

/**
  * Aplicativo para ETL do InovaTCE/Uso Veicular
  */
object Application extends App {

  @transient implicit lazy val session =
    SparkSession.builder().master("local").appName("vusage").config("spark.driver.memory ", "500000000").getOrCreate()

  implicit val system = ActorSystem("InovaTCE")
  implicit val executor = system.dispatcher
  implicit val mat = ActorMaterializer()
  implicit val timeout = Timeout(3.seconds)

  lazy val fileActor = system.actorOf(Props(new XmlActor) , "_xml")

  val files = {
    session.sparkContext.binaryFiles("/Users/mauriciofernandesdecastro/Desktop/2016/*Combustivel.zip")
      .flatMap { case (zipFilePath, zipContent) =>
        val zipInputStream = new ZipInputStream(zipContent.open())
        Stream.continually(zipInputStream.getNextEntry)
          .takeWhile(_ != null)
          .map { _ =>
            scala.io.Source.fromInputStream(zipInputStream, "UTF-16").getLines.mkString("\n")
          }
      }.take(10).foreach(fileActor ! _)
  }

}
