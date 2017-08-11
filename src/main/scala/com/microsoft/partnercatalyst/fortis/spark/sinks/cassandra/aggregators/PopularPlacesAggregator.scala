package com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra.aggregators
import com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra.CassandraPopularPlaces
import com.microsoft.partnercatalyst.fortis.spark.sinks.cassandra.dto._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

class PopularPlacesAggregator extends FortisAggregatorBase with Serializable{
  private val TargetTableName = "popularplaces"
  private val GroupedBaseColumnNames = Seq("placeid", "periodtype", "period", "conjunctiontopic1", "conjunctiontopic2", "conjunctiontopic3", "periodstartdate", "periodenddate", "centroidlat", "centroidlon")

  private def DetailedAggregateViewQuery: String = {
    val GroupedColumns = GroupedBaseColumnNames ++ Seq("pipelinekey", "externalsourceid")
    val SelectClause = GroupedColumns.mkString(",")

      s"SELECT $SelectClause, $AggregateFunctions " +
      s"FROM   $DfTableNameFlattenedEvents " +
      s"GROUP BY $SelectClause"
  }

  private def AllSourcesAggregateViewQuery: String = {
    val GroupedColumns = GroupedBaseColumnNames ++ Seq("pipelinekey")
    val SelectClause = GroupedColumns.mkString(",")

    s"SELECT $SelectClause, 'all' as externalsourceid, $AggregateFunctions " +
    s"FROM   $DfTableNameFlattenedEvents " +
    s"GROUP BY $SelectClause"
  }

  private def AllPipelineKeysAggregateViewQuery: String = {
    val GroupedColumns = GroupedBaseColumnNames
    val SelectClause = GroupedColumns.mkString(",")

    s"SELECT $SelectClause, 'all' as externalsourceid, 'all' as pipelinekey, $AggregateFunctions " +
    s"FROM   $DfTableNameFlattenedEvents " +
    s"GROUP BY $SelectClause "
  }

  private def IncrementalUpdateQuery: String = {
    val GroupedColumns = GroupedBaseColumnNames ++ Seq("pipelinekey", "externalsourceid")
    val SelectClause = GroupedColumns.mkString(",a.")

    s"SELECT a.$SelectClause, " +
    s"       $IncrementalUpdateMentionsUDF, $IncrementalUpdateSentimentUDF " +
    s"FROM   $DfTableNameComputedAggregates a "
  }

  override def FortisTargetTablename: String = TargetTableName

  override def flattenEvents(session: SparkSession, eventDS: Dataset[Event]): DataFrame = {
    import session.implicits._
    eventDS.flatMap(CassandraPopularPlaces(_)).toDF()
  }

  override def IncrementalUpdate(session: SparkSession, aggregatedDS: DataFrame): DataFrame = {
    aggregatedDS.createOrReplaceTempView(FortisTargetTablename)
    val cassandraSave = session.sqlContext.sql(IncrementalUpdateQuery)

    cassandraSave
  }

  override def AggregateEventBatches(session: SparkSession, flattenedEvents: DataFrame): DataFrame = {
    val detailedAggDF = session.sqlContext.sql(DetailedAggregateViewQuery)
    val allSourcesAggDF = session.sqlContext.sql(AllSourcesAggregateViewQuery)
    val allPipelinesAggDF = session.sqlContext.sql(AllPipelineKeysAggregateViewQuery)
    val unionedResults = detailedAggDF.union(allSourcesAggDF).union(allPipelinesAggDF)

    unionedResults
  }
}

