package com.microsoft.partnercatalyst.fortis.spark

import java.util.UUID

import com.microsoft.partnercatalyst.fortis.spark.dto.{Analysis, Details, FortisEvent, Location}

case class TestFortisEvent(
  details: Details,
  analysis: Analysis
) extends FortisEvent

case class TestFortisDetails(
  id: UUID,
  createdAtEpoch: Long,
  body: String,
  title: String,
  publisher: String,
  sourceUrl: String,
  sharedLocations: List[Location] = List()
) extends Details