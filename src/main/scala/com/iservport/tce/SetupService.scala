package com.iservport.tce

import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.sql.SparkSession

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

    val price    = toDataFrame("price")
    val quantity = toDataFrame("quantity")
    val ibge     = toDataFrame("ibge").select($"cdIBGE", $"populacao", $"area")

    // início das agregações

    import org.apache.spark.sql.functions._

    val pr = price
      .groupBy($"nrAnoLiquidacao", $"nrMesLiquidacao", $"idEntidade", $"dsTipoObjetoDespesa")
      .agg(
        first("nmMunicipio") as "nmMunicipio"
        , avg("vlUnitarioLiquidacao") as "vlMedioLiquidacao"
      )
      .persist
    val qs = quantity
      .groupBy($"nrAnoConsumo", $"nrMesConsumo", $"idEntidade", $"dsTipoObjetoDespesaNrSeq")
      .agg(
        first("cdIBGE") as "cdIBGE"
        , sum("nrQuantidadeConsumoVeiculoEqNrSeq") as "nrQuantidadeConsumoVeiculoMensal"
      ).filter($"nrQuantidadeConsumoVeiculoMensal" > 0)
      .persist

    // cria um novo dataframe com a combinação dos anteriores

    val expenditure = qs
      .join(pr,
          $"nrAnoLiquidacao"     === $"nrAnoConsumo" &&
          $"nrMesLiquidacao"     === $"nrMesConsumo" &&
          pr.col("idEntidade")   === qs.col("idEntidade") &&
          $"dsTipoObjetoDespesa" === $"dsTipoObjetoDespesaNrSeq",
        "left_outer"
      )
      .withColumn("nmMunicipio", $"nmMunicipio")
      .withColumn("vlMensalDespesa", $"vlMedioLiquidacao" * $"nrQuantidadeConsumoVeiculoMensal")
      .join(ibge, "cdIBGE")
      .withColumn("porArea", $"vlMensalDespesa" / $"area")
      .withColumn("porHabitante", $"vlMensalDespesa" / $"populacao")
      .toDF

    // persiste, reescrevendo a coleção de despesas (expenditure) quando necessário.

    val writeConfig = WriteConfig(Map("uri" -> s"$mongoUri.expenditure"))
    MongoSpark.save(expenditure.write.mode("overwrite"), writeConfig)

  }

}
