package com.microsoft.partnercatalyst.fortis.spark.analyzer

import java.time.Instant.now
import java.util.UUID.randomUUID

import com.microsoft.partnercatalyst.fortis.spark.transforms.image.ImageAnalyzer
import com.microsoft.partnercatalyst.fortis.spark.transforms.language.LanguageDetector
import twitter4j.{Status => TwitterStatus}

@SerialVersionUID(100L)
class TwitterAnalyzer extends Analyzer[TwitterStatus] with Serializable
  with AnalysisDefaults.EnableAll[TwitterStatus] {
  override def toSchema(item: TwitterStatus, locationFetcher: LocationFetcher, imageAnalyzer: ImageAnalyzer): ExtendedDetails[TwitterStatus] = {
    ExtendedDetails(
      id = randomUUID(),
      createdAtEpoch = now.getEpochSecond,
      body = item.getText,
      title = "",
      publisher = "Twitter",
      sourceUrl = s"https://twitter.com/statuses/${item.getId}",
      sharedLocations = Option(item.getGeoLocation) match {
        case Some(location) => locationFetcher(location.getLatitude, location.getLongitude).toList
        case None => List()
      },
      original = item
    )
  }

  override def detectLanguage(details: ExtendedDetails[TwitterStatus], languageDetector: LanguageDetector): Option[String] = {
    Option(details.original.getLang) match {
      case Some(lang) => Some(lang)
      case None => super.detectLanguage(details, languageDetector)
    }
  }
}