package org.cubefriendly

import java.io.File

import org.cubefriendly.data.{QueryBuilder, Cube}
import org.mapdb.{DBMaker, DB}
import org.specs2.mutable.Specification

/**
 * Cubefriendly
 * Created by david on 03.03.15.
 */
class LoadFromCsvSpec extends Specification{
  "A Cube data" should {
    def db():DB = DBMaker.newTempFileDB().lockThreadUnsafeEnable().make()

    "be loaded from CSV" in {
      val cubeName = "test_cube"
      val mapdb = db()
      val csvFile = new File("src/test/resources/test.csv")
      val actual: Cube = Cube.builder(mapdb)
        .csv(file = csvFile, separator = ",")
        .metrics("IMD SCORE","INCOME SCORE","EMPLOYMENT SCORE","HEALTH DEPRIVATION AND DISABILITY SCORE","EDUCATION SKILLS AND TRAINING SCORE","BARRIERS TO HOUSING AND SERVICES SCORE","CRIME AND DISORDER SCORE","LIVING ENVIRONMENT SCORE","Indoors Sub-domain Score","Outdoors Sub-domain Score","Geographical Barriers Sub-domain Score","Wider Barriers Sub-domain Score","Children/Young People Sub-domain Score","Skills Sub-domain Score")
        .toCube(cubeName)
      actual.name must be equalTo cubeName
      val result = QueryBuilder.query(actual).run().next()

      mapdb.close()
      success
    }
  }

}
