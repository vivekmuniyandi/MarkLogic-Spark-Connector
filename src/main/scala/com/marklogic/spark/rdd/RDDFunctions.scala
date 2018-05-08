package com.marklogic.spark.rdd

import com.marklogic.client.io.{DOMHandle, Format}
import com.marklogic.spark.marklogic.SparkDocument
import com.marklogic.spark.utils.DocumentWriter
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions

class MarklogicRDDFunctions(@transient val rdd: RDD[SparkDocument])
  extends Serializable {
  def saveRDDToMarkLogic(collectionName: String ="", directoryName: String =""): Unit = {
    val writer =  new DocumentWriter[SparkDocument](rdd.sparkContext.getConf,
      collectionName, directoryName)
    rdd.sparkContext.runJob(rdd, writer.write _)
  }

/*  def getDocumentPair(): RDD[(String, Object)] = {
    rdd.filter(x => x.getFormat().equals(Format.XML)).map(x => (x.getUri, x.getContent(new DOMHandle()).get()))
  }*/

  /*
  def getJSONDocumentPair(): RDD[(String, JsonNode)] = {
    rdd.filter(x => x.getFormat().equals(Format.JSON)).map(x => (x.getUri, x.getContent(new JacksonHandle().get())))
  }

  def getTextDocumentPair(): RDD[(String, String)] = {
    rdd.filter(x => x.getFormat().equals(Format.TEXT)).map(x => (x.getUri, x.getContent(new StringHandle().get())))
  }*/
  // Write other functions to get the metadata and content to enable actions
  // on them. If needed create URI RDD/ JSON RDD and XML RDD
}

class PairRDDFunctions(@transient val rdd: RDD[SparkDocument]) extends
  Serializable {
  def saveRDDToMarkLogic(collectionName: String ="", directoryName: String =""): Unit = {
    val writer =  new DocumentWriter[Object](rdd.sparkContext.getConf,
      collectionName, directoryName)
    rdd.sparkContext.runJob(rdd, writer.write _)
  }
}