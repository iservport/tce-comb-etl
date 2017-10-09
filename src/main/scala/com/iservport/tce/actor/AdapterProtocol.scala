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
    ,"cdIBGE" -> s"${attributes.getOrElse("cdIBGE", "SemCdIBGE")}"
    ,"nmMunicipio" -> s"${attributes.getOrElse("nmMunicipio", "SemNmMunicipio")}"
    ,"idEntidade" -> s"${attributes.getOrElse("idEntidade", "SemIdEntidade")}"
    ,"nmEntidade" -> s"${attributes.getOrElse("nmEntidade", "SemNmEntidade")}"
    ,"cdBem" -> s"${attributes.getOrElse("cdBem", "")}"
    ,"dsTipoPropriedadeBem" -> s"${attributes.getOrElse("dsTipoPropriedadeBem", "")}"
    ,"nrPlaca" -> s"${attributes.getOrElse("nrPlaca", "SEM PLAC")}"
    ,"dsMarcaVeiculo" -> s"${attributes.getOrElse("dsMarcaVeiculo", "")}"
    ,"dsModeloFIPE" -> s"${attributes.getOrElse("dsModeloFIPE", "")}"
    ,"dsVeiculoEquipamento" -> s"${attributes.getOrElse("dsVeiculoEquipamento", "")}"
  )
}
//case class Usage(attributes: Map[String, String]) extends AdapterProtocol {
//  val ano = s"${attributes.getOrElse("nrAno", "SemNrAno")}"
//  val mes = s"${attributes.getOrElse("nrMes", "SemNrMes")}"
//  val id = s"${attributes.getOrElse("idVeiculoEquipamento", "")}:$ano:$mes"
//  val doc = BSONDocument(
//      "_id" -> id
//    ,"cdIBGE" -> s"${attributes.getOrElse("cdIBGE", "SemCdIBGE")}"
//    ,"nmMunicipio" -> s"${attributes.getOrElse("nmMunicipio", "SemNmMunicipio")}"
//    ,"idEntidade" -> s"${attributes.getOrElse("idEntidade", "SemIdEntidade")}"
//    ,"nmEntidade" -> s"${attributes.getOrElse("nmEntidade", "SemNmEntidade")}"
//    ,"nrAno" -> ano
//    ,"nrMes" -> mes
//    ,"idVeiculoEquipamento" -> s"${attributes.getOrElse("idVeiculoEquipamento", "")}"
//    ,"dsTiposObjetoDespesa" -> s"${attributes.getOrElse("dsTiposObjetoDespesa", "VAZIO")}"
//    ,"vlConsumo" -> attributes.getOrElse("vlConsumo", "0").toDouble
//    ,"idTipoMedidor" -> s"${attributes.getOrElse("idTipoMedidor", "SemMedidor")}"
//    ,"ultimoEnvioSIMAMNesteExercicio" -> s"${attributes.getOrElse("ultimoEnvioSIMAMNesteExercicio", "YYYY")}"
//  )
//}
case class Price(attributes: Map[String, String]) extends AdapterProtocol {
  val dtLiquidacao = s"${attributes.getOrElse("dtLiquidacao", "0").stripSuffix("T00:00:00")}"
  val dsTipoObjetoDespesa = s"${attributes.getOrElse("dsTipoObjetoDespesa", "VAZIO")}"
  val idEntidade = s"${attributes.getOrElse("idEntidade", "SemIdEntidade")}"
  val id = s"$idEntidade:$dtLiquidacao:$dsTipoObjetoDespesa"
  val vlLiquidacao = attributes.getOrElse("vlLiquidacao", "0").toDouble
  val nrQuantidadeConsolidadaLiquidacao = attributes.getOrElse("nrQuantidadeConsolidadaLiquidacao", "1").toDouble
  val formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd")
  def toMonth(date: String) = f"${LocalDate.parse(date, formatter).getMonthValue}%02d"
  val doc = BSONDocument(
    "_id" -> s"$id"
    ,"cdIBGE" -> s"${attributes.getOrElse("cdIBGE", "SemCdIBGE")}"
    ,"idEntidade" -> idEntidade
    ,"nrAnoLiquidacao" -> s"${attributes.getOrElse("nrAnoLiquidacao", "")}"
    ,"nrMesLiquidacao" -> s"${toMonth(dtLiquidacao)}"
    ,"dtLiquidacao" -> dtLiquidacao
    ,"dsTipoObjetoDespesa" -> dsTipoObjetoDespesa
    ,"vlLiquidacao" -> vlLiquidacao
    ,"nrQuantidadeConsolidadaLiquidacao" -> nrQuantidadeConsolidadaLiquidacao
    ,"vlUnitarioLiquidacao" -> vlLiquidacao / nrQuantidadeConsolidadaLiquidacao
  )
}

case class Quantity(attributes: Map[String, String]) extends AdapterProtocol {
  val nrAnoConsumo = s"${attributes.getOrElse("nrAnoConsumo", "")}"
  val nrMesConsumo = s"${attributes.getOrElse("nrMesConsumo", "")}"
  val idEntidade = s"${attributes.getOrElse("idEntidade", "SemIdEntidade")}"
  val idVeiculoEquipamento = s"${attributes.getOrElse("idVeiculoEquipamento", "")}"
  val dsTipoObjetoDespesaNrSeq = s"${attributes.getOrElse("dsTipoObjetoDespesaNrSeq", "VAZIO")}"
  val id = s"$idVeiculoEquipamento:$nrAnoConsumo:$nrMesConsumo:$dsTipoObjetoDespesaNrSeq"
  val doc = BSONDocument(
    "_id" -> s"$id"
    ,"cdIBGE" -> s"${attributes.getOrElse("cdIBGE", "SemCdIBGE")}"
    ,"idEntidade" -> idEntidade
    ,"idVeiculoEquipamento" -> idVeiculoEquipamento
    ,"nrAnoConsumo" -> nrAnoConsumo
    ,"nrMesConsumo" -> nrMesConsumo
    ,"dsTipoObjetoDespesaNrSeq" -> dsTipoObjetoDespesaNrSeq
    ,"nrQuantidadeConsumoVeiculoEqNrSeq" -> attributes.getOrElse("nrQuantidadeConsumoVeiculoEqNrSeq", "1").toDouble
  )
}
