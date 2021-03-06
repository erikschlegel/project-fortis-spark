package com.microsoft.partnercatalyst.fortis.spark.sources.streamfactories

import com.github.catalystcode.fortis.spark.streaming.reddit.dto.RedditObject
import com.github.catalystcode.fortis.spark.streaming.reddit.{RedditAuth, RedditUtils}
import com.microsoft.partnercatalyst.fortis.spark.sources.streamprovider.{ConnectorConfig, StreamFactory}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

class RedditStreamFactory extends StreamFactory[RedditObject] {
  override def createStream(streamingContext: StreamingContext): PartialFunction[ConnectorConfig, DStream[RedditObject]] = {
    case ConnectorConfig("RedditObject", params) =>
      val auth = RedditAuth(params("applicationId"), params("applicationSecret"))
      val keywords = params("keywords").split('|')

      val subreddit = params.get("subreddit")
      val searchLimit = params.getOrElse("searchLimit", "25").toInt
      val searchResultType = Some(params.getOrElse("searchResultType", "link"))
      RedditUtils.createPageStream(
        auth,
        keywords.toSeq,
        streamingContext,
        subredit = subreddit,
        searchLimit = searchLimit,
        searchResultType = searchResultType
      )
  }
}
