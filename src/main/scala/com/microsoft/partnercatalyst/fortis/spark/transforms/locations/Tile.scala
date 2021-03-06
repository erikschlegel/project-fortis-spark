package com.microsoft.partnercatalyst.fortis.spark.transforms.locations

class TileId(var tileId: String) {
  var row: Int = -1
  var zoom: Int = -1
  var column: Int = -1

  private val parts = tileId.split('_')

  if (parts.length != 3){
    throw new IllegalArgumentException("Invalid tile ID format. Expecting {zoom_row_column}, i.e. '10_1232_21'")
  }

  private val tile = (parts(0).toInt, parts(1).toInt, parts(2).toInt)

  this.zoom = tile._1
  this.row = tile._2
  this.column = tile._3
}

class Tile(var tileId: TileId, var latitudeNorth: Double, var latitudeSouth: Double, var longitudeWest: Double,
           var longitudeEast: Double){
  def centerLatitude(): Double = (latitudeNorth + latitudeSouth) / 2.0
  def centerLongitude(): Double = (latitudeNorth + latitudeSouth) / 2.0
  def midNorthLatitude(): Double = (centerLatitude() + latitudeNorth) / 2.0
  def midSouthLatitude(): Double = (centerLatitude() + latitudeSouth) / 2.0
  def midEastLongitude(): Double = (centerLongitude() + longitudeEast) / 2.0
  def midWestLongitude(): Double = (centerLongitude() + longitudeWest) / 2.0
}

/**
  * Created by erisch on 5/24/2017.
  */

class TileUtils {
  final val MAX_ZOOM = 16
  final val MIN_ZOOM = 0

  def tile_id_from_lat_long(latitude: Double, longitude: Double, zoom: Int): TileId = {
    val row = row_from_latitude(latitude, zoom).toInt
    val column = column_from_longitude(longitude, zoom).toInt

    tile_id_from_row_column(row, column, zoom)
  }

  def row_from_latitude(latitude: Double, zoom: Int): Double = {
    math.floor((1 - math.log(math.tan(latitude * math.Pi / 180) + 1 / math.cos(latitude * math.Pi / 180)) / math.Pi) / 2 * math.pow(2, zoom))
  }

  def column_from_longitude(longitude: Double, zoom: Int): Double= {
    math.floor((longitude + 180.0) / 360.0 * math.pow(2, zoom))
  }

  def latitude_from_row(row: Int, zoom: Int): Double = {
    val n = math.Pi - 2.0 * math.Pi * row / math.pow(2, zoom)
    180.0 / math.Pi * math.atan(0.5 * (math.exp(n) - math.exp(-n)))
  }

  def longitude_from_column(column: Int, zoom: Int): Double = {
    column.toFloat / math.pow(2, zoom) * 360.0 - 180.0
  }

  def tile_from_tile_id(tileId: TileId): Tile = {
    new Tile(tileId, latitude_from_row(tileId.row, tileId.zoom),
      latitude_from_row(tileId.row + 1, tileId.zoom), longitude_from_column(tileId.column, tileId.zoom),
      longitude_from_column(tileId.column + 1, tileId.zoom))
  }

  def tile_id_from_row_column(row: Int, column: Int, zoom: Int ): TileId = {
    val tileStr = printf("%d_%d_%d", zoom, row, column)
    new TileId(tileStr.toString)
  }

  def parent_id(tile: Tile): TileId = {
    tile_id_from_lat_long(tile.centerLatitude(), tile.centerLongitude(), tile.tileId.zoom - 1)
  }

  def parent(tile: Tile): Tile = {
    tile_from_tile_id(parent_id(tile))
  }

  def tile_ids_for_all_zoom_levels(tileId: TileId): IndexedSeq[TileId] = {
    val tile = tile_from_tile_id(tileId)
    for (zoom <- MIN_ZOOM to MAX_ZOOM) yield tile_id_from_lat_long(tile.centerLatitude(), tile.centerLongitude(), zoom)
  }

  def children(tile: Tile): List[TileId] = {
    List( tile_id_from_lat_long(tile.midNorthLatitude(), tile.midEastLongitude(), tile.tileId.zoom + 1)
    ,
    tile_id_from_lat_long(tile.midNorthLatitude(), tile.midWestLongitude(), tile.tileId.zoom + 1)
    ,
    tile_id_from_lat_long(tile.midSouthLatitude(), tile.midEastLongitude(), tile.tileId.zoom + 1)
    ,
    tile_id_from_lat_long(tile.midSouthLatitude(), tile.midWestLongitude(), tile.tileId.zoom + 1)
    )
  }
}
