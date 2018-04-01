package com.marklogic.spark

import com.marklogic.client.document.DocumentRecord
import org.apache.spark.rdd.RDD

import scala.language.implicitConversions

class RDDFunctions(@transient val rdd: MarkLogicDocumentRDD)
  extends Serializable {
  def saveRDDToMarkLogic(collectionName: String ="", directoryName: String =""): Unit = {
    val writer =  new DocumentWriter[DocumentRecord](rdd.sparkContext.getConf,
      collectionName, directoryName)
    rdd.sparkContext.runJob(rdd, writer.write _)
  }

  // Write other functions to get the metadata and content to enable actions
  // on them. If needed create URI RDD/ JSON RDD and XML RDD
}