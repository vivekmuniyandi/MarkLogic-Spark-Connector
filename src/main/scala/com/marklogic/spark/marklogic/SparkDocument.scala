package com.marklogic.spark.marklogic

import com.fasterxml.jackson.databind.JsonNode
import com.marklogic.client.document.DocumentRecord
import com.marklogic.client.io.{DOMHandle, Format, JacksonHandle, StringHandle}
import com.marklogic.client.io.marker.{AbstractWriteHandle, DocumentMetadataReadHandle}
import org.w3c.dom.Document

class SparkDocument extends Serializable {
  var uri : String = _
  var content : AbstractWriteHandle = _
  var metadata : DocumentMetadataReadHandle = _
  var format : Format = _

  def this(uri: String,  content: AbstractWriteHandle, format: Format) {
    this()
    this.uri = uri
    this.content = content
    this.format = format
  }

  def this(record: DocumentRecord) = {
    this()
    var temp : AbstractWriteHandle = null
    record.getFormat match {
      case Format.XML => temp = record.getContent(new DOMHandle())
      case Format.JSON => temp = record.getContent(new JacksonHandle())
      case Format.TEXT => temp = record.getContent(new StringHandle())
    }
    uri = record.getUri
    content = temp
    format = record.getFormat
  }

  def getContentHandle(): AbstractWriteHandle = content

  def getContentDocument() : Object = {
    var document : Object = null
    format match {
      case Format.XML => document = content.asInstanceOf[DOMHandle].get()
      case Format.JSON => document = content.asInstanceOf[JacksonHandle].get()
      case Format.TEXT => document = content.asInstanceOf[StringHandle].get()
    }
    document
  }

  def getUri() : String = uri

  def getFormat() : Format = format
  // TODO: Add metadata as XML or JSON
}
