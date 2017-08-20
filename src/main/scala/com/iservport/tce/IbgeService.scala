package com.iservport.tce

import java.text.{DecimalFormat, NumberFormat}
import java.util.Locale

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.SparkSession

object IbgeService {

  import ApplicationConfig._

  def run(implicit session: SparkSession) = {

    import session.implicits._

    val ibge = session.read.option("header","true").option("sep", ";")
      .csv(ibgeCsv).as[IBGERead].map(toIBGE)
    val writeConfig = WriteConfig(Map("uri" -> s"$mongoUri.ibge"))
    MongoSpark.save(ibge.write.mode("overwrite"), writeConfig)
  }

  private def toIBGE(input: IBGERead): IBGE = {
    println(input)
    val f = NumberFormat.getInstance(new Locale("pt", "BR")).asInstanceOf[DecimalFormat]
    IBGE(input.nmMunicipio
      , input.cdIBGEcomDigito.substring(0, 6)
      , input.gentilico
      , f.parse(input.populacao).intValue()
      , f.parse(input.area).doubleValue()
      , f.parse(input.densidadeDemografica).doubleValue()
      , f.parse(input.valorAdicionadoBrutoTotal).doubleValue()
    )
  }

}
case class IBGERead
(
  nmMunicipio: String
, cdIBGEcomDigito: String
, gentilico: String
, populacao: String
, area: String
, densidadeDemografica: String
, valorAdicionadoBrutoTotal: String
)
case class IBGE
(
  nmMunicipio: String
  , cdIBGE: String
  , gentilico: String
  , populacao: Int
  , area: Double
  , densidadeDemografica: Double
  , valorAdicionadoBrutoTotal: Double
)