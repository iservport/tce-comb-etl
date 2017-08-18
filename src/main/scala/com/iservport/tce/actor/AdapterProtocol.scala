package com.iservport.tce.actor

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import reactivemongo.bson.BSONDocument


/**
  * Created by mauriciofernandesdecastro on 29/05/17.
  */
sealed trait AdapterProtocol {
  val  doc: BSONDocument
}
case class CityData(attributes: Map[String, String]) extends AdapterProtocol {
  val id = attributes.getOrElse("cdIBGE", "SemIBGE")
  val doc = BSONDocument(
    "_id" -> s"$id"
    ,"cdIBGE" -> s"$id"
    ,"nmMunicipio" -> s"${attributes.getOrElse("nmMunicipio", "")}"
  )
}
case class CityGeo(doc: BSONDocument)
case class EntityData(attributes: Map[String, String]) extends AdapterProtocol {
  val id = attributes.getOrElse("idEntidade", "SemEntidade")
  val doc = BSONDocument(
    "_id" -> s"$id"
    ,"cdIBGE" -> s"${attributes.getOrElse("cdIBGE", "SemIBGE")}"
    ,"nmEntidade" -> s"${attributes.getOrElse("nmEntidade", "")}"
  )
}
case class Vehicle(attributes: Map[String, String]) extends AdapterProtocol {
  val id = attributes.getOrElse("idVeiculoEquipamento", "")
  val doc = BSONDocument(
    "_id" -> s"$id"
    ,"cdIBGE" -> s"${attributes.getOrElse("cdIBGE", "SemIBGE")}"
    ,"idEntidade" -> s"${attributes.getOrElse("idEntidade", "")}"
    ,"cdBem" -> s"${attributes.getOrElse("cdBem", "")}"
    ,"dsTipoPropriedadeBem" -> s"${attributes.getOrElse("dsTipoPropriedadeBem", "")}"
    ,"nrPlaca" -> s"${attributes.getOrElse("nrPlaca", "SEM PLAC")}"
    ,"dsMarcaVeiculo" -> s"${attributes.getOrElse("dsMarcaVeiculo", "")}"
    ,"dsModeloFIPE" -> s"${attributes.getOrElse("dsModeloFIPE", "")}"
    ,"dsVeiculoEquipamento" -> s"${attributes.getOrElse("dsVeiculoEquipamento", "")}"
  )
}
case class Usage(attributes: Map[String, String]) extends AdapterProtocol {
  val id = s"${attributes.getOrElse("idVeiculoEquipamento", "")}:${attributes.getOrElse("DataReferencia", "")}"
  val doc = BSONDocument(
      "_id" -> s"$id"
    ,"year" -> s"${attributes.getOrElse("ultimoEnvioSIMAMNesteExercicio", "YYYY").split("/").head}"
    ,"cdIBGE" -> s"${attributes.getOrElse("cdIBGE", "SemIBGE")}"
    ,"idEntidade" -> s"${attributes.getOrElse("idEntidade", "")}"
    ,"idVeiculoEquipamento" -> s"${attributes.getOrElse("idVeiculoEquipamento", "")}"
    ,"dsTiposObjetoDespesa" -> s"${attributes.getOrElse("dsTiposObjetoDespesa", "Vazio")}"
    ,"vlConsumo" -> attributes.getOrElse("vlConsumo", "0").toDouble
    ,"idTipoMedidor" -> s"${attributes.getOrElse("idTipoMedidor", "SemMedidor")}"
  )
}
case class Quantity(attributes: Map[String, String]) extends AdapterProtocol {
  val id = s"${attributes.getOrElse("idEntidade", "SemEntidade")}:${attributes.getOrElse("dtLiquidacao", "0").stripSuffix("T00:00:00")}"
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")
  def toMonth(date: String) = f"${LocalDate.parse(date, formatter).getMonthValue}%02d"
  val doc = BSONDocument(
    "_id" -> s"$id"
    ,"cityId" -> s"${attributes.getOrElse("cdIBGE", "")}"
    ,"entityId" -> s"${attributes.getOrElse("idEntidade", "")}"
    ,"year" -> s"${attributes.getOrElse("nrAnoLiquidacao", "")}"
    ,"month" -> s"${toMonth(attributes.getOrElse("dtLiquidacao", ""))}"
    ,"subject" -> s"${attributes.getOrElse("dsTipoObjetoDespesa", "")}:${attributes.getOrElse("dsUnidadeMedida", "")}"
    ,"averagePrice" -> attributes.getOrElse("vlLiquidacao", "0").toDouble / attributes.getOrElse("nrQuantidadeConsolidadaLiquidacao", "1").toDouble
  )
}

//case class VehicleData(attributes: Map[String, String]) extends DomainProtocol {
//  val json = s"""{
//                   |"_id": "${attributes.getOrElse("nrRenavam", "")}"
//                   |,"code": "${attributes.getOrElse("cdBem", "")}"
//                   |,"fuel": "${attributes.getOrElse("dsCombustivelVeiculoEq", "")}"
//                   |}""".stripMargin
//}
