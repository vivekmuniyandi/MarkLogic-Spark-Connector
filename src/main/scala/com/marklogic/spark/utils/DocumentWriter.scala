package com.marklogic.spark.utils

import java.io.Serializable

import com.fasterxml.jackson.databind.JsonNode
import com.marklogic.client.datamovement._
import com.marklogic.client.document.DocumentRecord
import com.marklogic.client.io.marker.AbstractWriteHandle
import com.marklogic.client.io._
import com.marklogic.client.{DatabaseClient, DatabaseClientFactory}
import com.marklogic.spark.marklogic.SparkDocument
import org.apache.spark.internal.Logging
import org.apache.spark.{SparkConf, TaskContext}
import org.w3c.dom.Document

class DocumentWriter[T](@transient conf : SparkConf, collection :String,
                        directory : String) extends Serializable with Logging{

  val mlHost = conf.get("MarkLogic_Host", "localhost")
  val mlPort : Int = conf.getInt("MarkLogic_Port", 8000)
  val mlUser = conf.get("MarkLogic_User")
  val mlPwd = conf.get("MarkLogic_Password")
  var uriPrefix = "/"
  if(!directory.equals("")) {
    uriPrefix = uriPrefix + directory + "/"
  } else {
    uriPrefix = uriPrefix
  }

  def write(taskContext: TaskContext, data: Iterator[T]): Unit = {

        val client: DatabaseClient  = DatabaseClientFactory.newClient(
          mlHost,
          mlPort,
          mlUser,
          mlPwd,
          DatabaseClientFactory.Authentication.valueOf("DIGEST"))

        val moveMgr:DataMovementManager = client.newDataMovementManager()

        val batcher : WriteBatcher  = moveMgr.newWriteBatcher().
                                                  withJobName("RDBBatchDataMover").
                                                  onBatchSuccess(batchSuccessListener).
                                                  onBatchFailure(batchFailureListener)

        val ticket : JobTicket = moveMgr.startJob(batcher)
        var metadataHandle : DocumentMetadataHandle= null
        while(data.hasNext){
          val docRecord = data.next().asInstanceOf[SparkDocument]
          //val isPair : Boolean = rddVal.isInstanceOf
          val id = uriPrefix+docRecord.getUri

          // TODO: Need to add the collection to the existing metadata and not replace them
          if(!collection.equals("")) {
            metadataHandle = new DocumentMetadataHandle()
            metadataHandle.getCollections.add(collection)
            batcher.add(id, metadataHandle, docRecord.getContentHandle())
          } else {
        //    batcher.add(id, docRecord.getMetadata(new StringHandle()),
          //    docRecord.getContent(new StringHandle().withFormat(docRecord
            //  .getFormat)))
            batcher.add(id, docRecord.getContentHandle())
          }
          println(id)
        }
        batcher.flushAndWait()
        moveMgr.stopJob(ticket)
  }

  object batchSuccessListener extends WriteBatchListener{
    override def processEvent( batch: WriteBatch) : Unit =
      logInfo(f"Sucessfully wrote " + batch.getItems().length)
  }

  object batchFailureListener extends WriteFailureListener{
    override def processFailure(batch: WriteBatch, failure: Throwable): Unit = {
      logError("FAILURE on batch:" + batch.getJobTicket.getJobId + failure.toString)
    }
  }

}