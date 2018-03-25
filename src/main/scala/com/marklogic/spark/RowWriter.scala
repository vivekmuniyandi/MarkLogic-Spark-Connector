package com.marklogic.spark

import java.io.Serializable

import com.marklogic.client.io.{Format, StringHandle}
import com.marklogic.client.{DatabaseClientFactory, DatabaseClient}
import com.marklogic.client.datamovement._
import org.apache.spark.sql.types.{DataType, StructField}
import org.apache.spark.{TaskContext, SparkConf}
import org.apache.spark.sql.Row
import org.apache.spark.internal.Logging

/**
 * Created by hpuranik on 6/7/2016.
 */
class RowWriter[T](@transient conf : SparkConf) extends Serializable with Logging{

  val mlHost = conf.get("MarkLogic_Host", "localhost")
  val mlPort : Int = conf.getInt("MarkLogic_Port", 8000)
  val mlDatabaseName: String = conf.get("MarkLogic_Database")
  val mlUser = conf.get("MarkLogic_User")
  val mlPwd = conf.get("MarkLogic_Password")


  def write(taskContext: TaskContext, data: Iterator[T]): Unit = {

    val secCtx: DatabaseClientFactory.SecurityContext = new DatabaseClientFactory.DigestAuthContext(mlUser, mlPwd)
    val client: DatabaseClient = DatabaseClientFactory.newClient(mlHost, mlPort, mlDatabaseName, secCtx)

    val moveMgr:DataMovementManager = client.newDataMovementManager()

    val batcher : WriteBatcher  = moveMgr.newWriteBatcher().
      withJobName("RDBBatchDataMover").
      onBatchSuccess(batchSuccessListener).
      onBatchFailure(batchFailureListener)

    val ticket : JobTicket = moveMgr.startJob(batcher)

    while(data.hasNext){
      val row : Row = data.next().asInstanceOf[Row]
      val id : String = row2Key(row)
      val doc : String = row2JSON(row)
      batcher.add(id, new StringHandle(doc).withFormat(Format.JSON))
      //println("URI = " + id + ": Doc = " + doc)
    }
    batcher.awaitCompletion()
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
  def row2Key(row : Row): String = {
    row.getAs(0).toString
  }


  def row2JSON(row : Row): String = {
    val fieldNames : Array[String] = row.schema.fieldNames
    val fieldStructs : Array[StructField] = row.schema.fields

    val fieldIt : Iterator[StructField] = fieldStructs.iterator

    val builder : StringBuilder = new StringBuilder
    builder.append("{ ")
    while(fieldIt.hasNext) {
      val field : StructField = fieldIt.next()
      val columnName: String = field.name
      val columnType: DataType = field.dataType
      var columnVal:String = null
      if(row.isNullAt(row.fieldIndex(columnName))){
        columnVal = ""
      } else{
        columnVal = row.getAs(columnName).toString
      }
      //println("ColumnName = " + columnName + ": Type = " + columnType + ": Value = " + columnVal)

      //val columnVal: String = row.getAs[String](columnName)
      builder.append("\n\"").
        append(columnName).
        append("\": \"").
        append(columnVal.toString).
        append("\"")
      if (fieldIt.hasNext) {
        builder.append(",")
      }
    }
    builder.append("\n}")
    builder.toString()
  }


}

