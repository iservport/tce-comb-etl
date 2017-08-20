package com.iservport.tce

import org.apache.spark.sql.SparkSession
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.{ReadConfig, WriteConfig}

/**
  * Serviço de ajuste incial dos dados.
  */
object SetupService {

  import com.iservport.tce.ApplicationConfig._

  /**
    * Executa o setup.
    */
  def run(implicit session: SparkSession) = {

    import session.implicits._

    // cache dos dataframes

    val usage    = toDataFrame("usage")
    val quantity = toDataFrame("quantity")
    val ibge = toDataFrame("ibge").select($"cdIBGE", $"populacao", $"area")

    // início das agregações

    import org.apache.spark.sql.functions._

    val us = usage
      .groupBy($"nrAno", $"nrMes", $"idEntidade", $"dsTiposObjetoDespesa")
      .agg(
          first("cdIBGE") as "cdIBGE"
        , first("nmMunicipio") as "nmMunicipio"
        , first("nmEntidade") as "nmEntidade"
        , sum("vlConsumo") as "vlMensalConsumo"
      )
      .persist
    val qs = quantity
      .groupBy($"nrAnoLiquidacao", $"nrMesLiquidacao", $"idEntidade", $"dsTipoObjetoDespesa")
      .agg(avg("vlUnitarioLiquidacao") as "vlMedioLiquidacao")
      .persist

    // facilita a leitura por nomes de colunas sem ambiguidades

    val idEntidade = us.col("idEntidade")
    val dsTiposObjetoDespesa = us.col("dsTiposObjetoDespesa")

    // cria um novo dataframe com a combinação dos anteriores

    val expenditure = us
      .join(qs, qs.col("nrAnoLiquidacao") === $"nrAno" && qs.col("nrMesLiquidacao") === $"nrMes" && qs.col("idEntidade") === idEntidade && qs.col("dsTipoObjetoDespesa").startsWith(dsTiposObjetoDespesa))
      .withColumn("vlMensalDespesa", $"vlMedioLiquidacao" * $"vlMensalConsumo")
      .join(ibge, "cdIBGE")
      .withColumn("porArea", $"vlMensalDespesa" / $"area")
      .withColumn("porHabitante", $"vlMensalDespesa" / $"populacao")
      .toDF

    // persiste, reescrevendo a coleção de despesas (expenditure) quando necessário.

    val writeConfig = WriteConfig(Map("uri" -> s"$mongoUri.expenditure"))
    MongoSpark.save(expenditure.write.mode("overwrite"), writeConfig)

  }

}
