package com.marklogic.spark

import com.marklogic.spark.rdd.MarkLogicDocumentRDD
import org.apache.spark.SparkContext

import scala.language.implicitConversions

class SparkContextFunctions (@transient val sc: SparkContext) extends Serializable {

  def newMarkLogicDocumentRDD(query : String):  MarkLogicDocumentRDD = {
    new MarkLogicDocumentRDD(sc, query)
  }
}
