package com.microsoft.partnercatalyst.fortis.spark.pipeline

import com.github.catalystcode.fortis.spark.streaming.instagram.dto.InstagramItem
import com.microsoft.partnercatalyst.fortis.spark.dto.AnalyzedItem
import com.microsoft.partnercatalyst.fortis.spark.streamprovider.{ConnectorConfig, StreamProvider}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

object InstagramPipeline extends Pipeline {
  override def apply(streamProvider: StreamProvider, streamRegistry: Map[String, List[ConnectorConfig]],
    ssc: StreamingContext, transformContext: TransformContext): Option[DStream[AnalyzedItem]] = {
    import transformContext._

    streamProvider.buildStream[InstagramItem](ssc, streamRegistry("instagram")).map(_
      .map(instagram => {
        // do computer vision analysis
        AnalyzedItem(
          body = instagram.caption.text,
          title = "",
          sharedLocations = instagram.location match {
            case Some(location) => locationsExtractor.fetch(location.latitude, location.longitude).toList
            case None => List()},
          analysis = imageAnalyzer.analyze(instagram.images.standard_resolution.url),
          source = instagram.link)
      })
      .map(analyzedItem => {
        // keyword extraction
        val keywords = keywordExtractor.extractKeywords(analyzedItem.body)
        analyzedItem.copy(analysis = analyzedItem.analysis.copy(keywords = keywords ::: analyzedItem.analysis.keywords))
      }))
  }
}