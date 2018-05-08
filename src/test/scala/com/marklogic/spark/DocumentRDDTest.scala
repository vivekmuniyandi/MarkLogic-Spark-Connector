package com.marklogic.spark

import java.util
import javax.xml.parsers.DocumentBuilderFactory

import com.fasterxml.jackson.databind.ObjectMapper
import com.marklogic.client.io.{DOMHandle, Format, JacksonHandle, StringHandle}
import com.marklogic.client.query.StructuredQueryBuilder
import com.marklogic.spark.marklogic.SparkDocument
import org.apache.spark.{Partition, SparkConf, SparkContext}
import org.scalatest.FunSuite
import org.w3c.dom.{Document, Element}

import scala.collection.JavaConverters

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
      val documents: Iterator[SparkDocument] = rdd.compute(part, null)
      var count: Int = 0
      while(documents.hasNext){
        val doc: SparkDocument = documents.next()
        println(doc.getUri + " " + doc.getContentDocument())
        count += 1
      }
      println("Computed Documents:= " + count)
    }
  }

  test("testDocumentRDDReadAndWrite") {
    val query = new StructuredQueryBuilder().directory(true, "/example/")
    val rdd = sc.newMarkLogicDocumentRDD(query.serialize())
    val uriRdd = rdd.map(x => x.getUri)
    uriRdd.foreach(println)
    rdd.saveRDDToMarkLogic("saveCollection", "newDirectory")
  }

  test("testCreateRDDandWriteToMarklogic") {
    // Create DOM Document
    var list : util.ArrayList[SparkDocument] = new util.ArrayList[SparkDocument]()
    val domDocument: Document = DocumentBuilderFactory.newInstance.newDocumentBuilder.newDocument
    val root: Element = domDocument.createElement("root")
    root.setAttribute("xml:lang", "en")
    root.setAttribute("foo", "bar")
    root.appendChild(domDocument.createElement("child"))
    root.appendChild(domDocument.createTextNode("mixed"))
    domDocument.appendChild(root)
    var domHandle : DOMHandle = new DOMHandle(domDocument)

    // Create JSON Document
    val docText : String = "{\"name\" : \"John\", \"lname\" : \"Jenny\", " +
      "\"name\" : \"Steve\"}"
    val mapper: ObjectMapper = new ObjectMapper
    var readNode = mapper.readTree(docText)

    val stringText = "A simple text document"
    list.add(new SparkDocument("/sample/1.xml", domHandle, Format.XML))
    list.add(new SparkDocument("/sample/2.json", new JacksonHandle(readNode), Format.JSON))
    list.add(new SparkDocument("/sample/3.txt", new StringHandle(stringText), Format.TEXT))

    var rdd = sc.parallelize(JavaConverters.asScalaIteratorConverter(list.iterator()).asScala.toSeq)
    rdd.saveRDDToMarkLogic()
  }
}
