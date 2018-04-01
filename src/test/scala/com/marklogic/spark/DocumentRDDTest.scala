package com.marklogic.spark

import com.marklogic.client.document.DocumentRecord
import com.marklogic.client.io.StringHandle
import com.marklogic.client.query.{StructuredQueryBuilder, StructuredQueryDefinition}
import org.apache.spark.{Partition, SparkConf, SparkContext}
import org.scalatest.FunSuite

class DocumentRDDTest extends FunSuite {

  val sparkConf: SparkConf = new SparkConf().setAppName("com.marklogic.spark.DocumentRDDTest").setMaster("local")
  sparkConf.set("MarkLogic_Host", "localhost")
  sparkConf.set("MarkLogic_Port", "8000")
  sparkConf.set("MarkLogic_Database", "Documents")
  sparkConf.set("MarkLogic_User", "admin")
  sparkConf.set("MarkLogic_Password", "admin")

  val sc: SparkContext = new SparkContext(sparkConf)

  test("testComputePartitions") {
    val query = new StructuredQueryBuilder().collection("TestCollection")
    val rdd = sc.newMarkLogicDocumentRDD(query.serialize())
    val parts: Array[Partition] = rdd.accessParts

    for(part <- parts){
      println(part.toString)
      val documents: Iterator[DocumentRecord] = rdd.compute(part, null)
      var count: Int = 0
      while(documents.hasNext){
        val doc: DocumentRecord = documents.next()
        println(doc.getUri + " " + doc.getContent(new StringHandle()).get())
        count += 1
      }
      println("Computed Documents:= " + count)
    }
  }

  test("testDocumentRDDWrite") {
    val query = new StructuredQueryBuilder().directory(true, "/example/")
    val rdd = sc.newMarkLogicDocumentRDD(query.serialize())
    val uriRdd = rdd.map(x => x.getUri)
    uriRdd.foreach(println)
    rdd.saveRDDToMarkLogic("saveCollection", "newDirectory")
  }
}
