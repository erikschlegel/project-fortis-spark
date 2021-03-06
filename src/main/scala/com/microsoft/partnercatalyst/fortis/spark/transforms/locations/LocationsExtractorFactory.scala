package com.microsoft.partnercatalyst.fortis.spark.transforms.locations

import com.microsoft.partnercatalyst.fortis.spark.dto.{Geofence, Location}
import com.microsoft.partnercatalyst.fortis.spark.logging.Loggable
import com.microsoft.partnercatalyst.fortis.spark.transforms.locations.client.FeatureServiceClient

import scala.collection.mutable

@SerialVersionUID(100L)
class LocationsExtractorFactory(
  featureServiceClient: FeatureServiceClient,
  geofence: Geofence) extends Serializable with Loggable {

  protected var lookup: Map[String, Set[String]] = _

  def buildLookup(): this.type = {
    val map = mutable.Map[String, mutable.Set[String]]()
    featureServiceClient.bbox(geofence).foreach(location => {
      val key = location.name.toLowerCase
      val value = location.id
      map.getOrElseUpdate(key, mutable.Set()).add(value)
    })

    lookup = map.map(kv => (kv._1, kv._2.toSet)).toMap
    logDebug(s"Built lookup for $geofence with ${lookup.size} locations")
    this
  }

  def create(placeRecognizer: Option[PlaceRecognizer] = None, ngrams: Int = 3): LocationsExtractor = {
    new LocationsExtractor(lookup, featureServiceClient, placeRecognizer, ngrams)
  }

  def fetch(latitude: Double, longitude: Double): Iterable[Location] = {
    val locationsForPoint = featureServiceClient.point(latitude = latitude, longitude = longitude)
    val locationsInGeofence = locationsForPoint.flatMap(location => lookup.get(location.name.toLowerCase)).flatten.toSet
    locationsInGeofence.map(wofId => Location(wofId, confidence = Some(1.0), latitude = Some(latitude), longitude = Some(longitude)))
  }
}
