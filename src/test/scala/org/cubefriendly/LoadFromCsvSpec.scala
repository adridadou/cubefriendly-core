package org.cubefriendly

import java.io.File

import org.cubefriendly.data.Cube
import org.cubefriendly.processors.{CubeConfig, DataProcessorProvider, DataProcessorProviderImpl}
import org.mapdb.{DB, DBMaker}
import org.specs2.mutable.Specification

/**
 * Cubefriendly
 * Created by david on 03.03.15.
 */
class LoadFromCsvSpec extends Specification{
  "A Cube data" should {
    def db():DB = DBMaker.tempFileDB().make()

    "be loaded from CSV" in {
      val cubeName = "test_cube"
      val mapdb = db()
      val provider: DataProcessorProvider = new DataProcessorProviderImpl()
      val csvFile = new File("src/test/resources/test.csv")
      val actual: Cube = provider.forSource(file = csvFile).process(
        db = mapdb,
        config = CubeConfig(
          name = cubeName,
          metrics = Seq("IMD SCORE", "INCOME SCORE", "EMPLOYMENT SCORE", "HEALTH DEPRIVATION AND DISABILITY SCORE", "EDUCATION SKILLS AND TRAINING SCORE", "BARRIERS TO HOUSING AND SERVICES SCORE", "CRIME AND DISORDER SCORE", "LIVING ENVIRONMENT SCORE", "Indoors Sub-domain Score", "Outdoors Sub-domain Score", "Geographical Barriers Sub-domain Score", "Wider Barriers Sub-domain Score", "Children/Young People Sub-domain Score", "Skills Sub-domain Score")))
      actual.name must be equalTo cubeName
      mapdb.close()
      success
    }
  }

}
