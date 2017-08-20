# tce-comb-etl
ETL para o aplicativo de Uso Veicular do Programa InovaTCE

O program executa em três etapas:

  * utiliza Apache Spark e Akka para descompactar, ler os arquivos selecionados e persistir,
  
  * lê dados do IBGE,
  
  * prepara o abiente, totalizando valores de gastos.
  
Os dados são lidos a partir do conjunto de arquivos xml fornecidos periodicamente
pelo TCE/PR.



