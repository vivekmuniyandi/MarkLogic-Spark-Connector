package com.marklogic.spark

import com.fasterxml.jackson.databind.JsonNode
import com.marklogic.client.DatabaseClient
import com.marklogic.client.DatabaseClientFactory
import com.marklogic.client.DatabaseClientFactory.DigestAuthContext
import com.marklogic.client.datamovement.DataMovementManager
import com.marklogic.client.document.{DocumentPage, DocumentRecord, GenericDocumentManager}
import com.marklogic.client.query.{QueryManager, RawCombinedQueryDefinition, StructuredQueryDefinition}
import com.marklogic.client.datamovement._
import com.marklogic.client.impl.GenericDocumentImpl
import com.marklogic.client.io.Format
import org.apache.spark.internal.Logging

import scala.collection.JavaConversions._
import scala.collection.{Set, mutable}
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

class MarkLogicPartition(val id: Int,
                         val uris: Array[String],
                         val host: String,
                         val forest: String,
                         val port: Int,
                         val dbName: String,
                         val userName: String,
                         val pwd: String,
                         val timeStamp: Long
                         ) extends Partition{

  override def index: Int = id
  override def toString = "index: " + index +
                          ", host: " + host +
                          ", forest: " + forest +
                          ", database: " + dbName +
                          ", URI Count: " + uris.length
}

class MarkLogicDocumentRDD(@transient sc: SparkContext, query: String) extends
  RDD[DocumentRecord](sc, Nil) with Logging {

  var partitionMap: mutable.HashMap[String, mutable.HashMap[String, ArrayBuffer[MarkLogicPartition]]] = null

  val mlHost = sc.getConf.get("MarkLogic_Host", "localhost")
  val mlPort: Int = sc.getConf.getInt("MarkLogic_Port", 8000)
  val mlDatabaseName: String = sc.getConf.get("MarkLogic_Database")
  val mlUser = sc.getConf.get("MarkLogic_User")
  val mlPwd = sc.getConf.get("MarkLogic_Password")

  def accessParts: Array[Partition] = getPartitions

  override protected def getPartitions: Array[Partition] = {

    partitionMap = new mutable.HashMap[String, mutable.HashMap[String, ArrayBuffer[MarkLogicPartition]]]

    val secCtx: DatabaseClientFactory.SecurityContext = new DatabaseClientFactory.DigestAuthContext(mlUser, mlPwd)
    val client: DatabaseClient = DatabaseClientFactory.newClient(mlHost, mlPort, mlDatabaseName, secCtx)
    val moveMgr: DataMovementManager = client.newDataMovementManager()
    val queryMgr : QueryManager = client.newQueryManager()
    val queryDef = queryMgr.newRawCombinedQueryDefinitionAs(Format.XML, query)
    val uriBatcher: QueryBatcher = moveMgr.newQueryBatcher(queryDef).
      withConsistentSnapshot().
      withJobName("RDD Creation").
      withBatchSize(100).
      onUrisReady(new batchReady).
      onQueryFailure(new queryFailed)

    val uriBatcherTicket: JobTicket = moveMgr.startJob(uriBatcher)
    uriBatcher.awaitCompletion()
    moveMgr.stopJob(uriBatcherTicket)

    /* organize all the partitions within a host in breadth first manner
       For example 3 hosts 3 forests and 3 partitions each
        H1 F1 P1
        H1 F2 P1
        H1 F3 P1
        H1 F1 P2
        H1 F2 P2
        H1 F3 P2
        H1 F1 P3
        H1 F2 P3
        H1 F3 P3
        H2 F1 P1
        H2 F2 P1
        H2 F3 P1
        H2 F1 P2
        H2 F2 P2
        H2 F3 P2
        H2 F1 P3
        H2 F2 P3
        H2 F3 P3
        H3 F1 P1
        H3 F2 P1
        H3 F3 P1
        H3 F1 P2
        H3 F2 P2
        H3 F3 P2
        H3 F1 P3
        H3 F2 P3
        H3 F3 P3
        partitionMap -> HashMap<Host, HashMap<Forest, ArrayBuffer<Partiton>>>

        For each batch, we create a partition in the QueryBatchListener

     */
    val hosts: Set[String] = partitionMap.keySet
    val hostCount: Int = hosts.size
    val hostSplits: Array[ArrayBuffer[MarkLogicPartition]] = new Array[ArrayBuffer[MarkLogicPartition]](hostCount)
    var hostIndex: Int = 0
    for (host <- hosts) {
      val forestSplitLists: mutable.HashMap[String,
        ArrayBuffer[MarkLogicPartition]] = partitionMap.getOrElse(host, null)
      // get the value HashMap<Forest, ArrayBuffer<Partiton>> for the host
      val hostForests: Set[String] = forestSplitLists.keySet // get the
      // forests for that host
      //walk through breadth first manner
      hostSplits(hostIndex) = new ArrayBuffer[MarkLogicPartition]
      var more: Boolean = true
      var distro: Int = 0
      while (more) {
        more = false
        for (hostForest <- hostForests) {
          val forestPartitions: ArrayBuffer[MarkLogicPartition] =
            forestSplitLists(hostForest) // forestPartitions is an
          // ArrayBuffer of the partitions which are batches
          if (distro < forestPartitions.size) {
            hostSplits(hostIndex) += forestPartitions.get(distro)
          }
          more = more || (distro + 1 < forestPartitions.size)
        }
        distro += 1
      }
      hostIndex += 1
    }

    val partitions: ArrayBuffer[MarkLogicPartition] = new ArrayBuffer[MarkLogicPartition]
    var more: Boolean = true
    var distro: Int = 0
    while (more) {
      more = false
      for (splitListPerHost <- hostSplits) {
        if (distro < splitListPerHost.size) {
          partitions.add(splitListPerHost.get(distro))
        }
        more = more || (distro + 1 < splitListPerHost.size)
      }
      distro += 1
    }
    partitions.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[DocumentRecord] = {

    val part: MarkLogicPartition = split.asInstanceOf[MarkLogicPartition]
    // fetch data from server
    val partitionDocuments: ArrayBuffer[DocumentRecord] = new ArrayBuffer[DocumentRecord]
    val secCtx: DatabaseClientFactory.SecurityContext =
      new DatabaseClientFactory.DigestAuthContext(part.userName, part.pwd)
    val client: DatabaseClient = DatabaseClientFactory.newClient(part.host, part.port, part.dbName, secCtx)
    val docMgr: GenericDocumentManager = client.newDocumentManager()
    val page: DocumentPage = docMgr.asInstanceOf[GenericDocumentImpl].read(part.timeStamp, part.uris: _*)

    //val result: EvalResultIterator = client.newServerEval.xquery(query.toString).eval
    while (page.hasNext) {
      val record: DocumentRecord = page.next()
      partitionDocuments.add(record)
    }

    partitionDocuments.iterator
  }

  class ForestSplit {
    private[MarkLogicDocumentRDD] var forestId: BigInt = null
    private[MarkLogicDocumentRDD] var hostName: String = null
    private[MarkLogicDocumentRDD] var recordCount: Long = 0L
  }

  class batchReady extends QueryBatchListener {
    override def processEvent(batch: QueryBatch): Unit = {
      val databaseClient: DatabaseClient = batch.getClient
      val idx: Int = batch.getJobBatchNumber.toInt - 1
      val host: String = batch.getForest.getHost
      val forest: String = batch.getForest.getForestName
      var forestParts: mutable.HashMap[String, ArrayBuffer[MarkLogicPartition]] = partitionMap.getOrDefault(host, null)
      //println(partitionMap.getClass.getCanonicalName)
      if (forestParts == null) {
        //host encountered for the first time
        forestParts = new mutable.HashMap[String, ArrayBuffer[MarkLogicPartition]]
        partitionMap.put(host, forestParts)
      }
      var parts: ArrayBuffer[MarkLogicPartition] = forestParts.getOrDefault(forest, null)
      if (parts == null) {
        //forest encountered for the first time
        parts = new ArrayBuffer[MarkLogicPartition]
        forestParts.put(forest, parts)
      }
      val timestamp: Long = batch.getServerTimestamp
      val securityContext = databaseClient.getSecurityContext.asInstanceOf[DigestAuthContext]
      parts.add(new MarkLogicPartition(idx,
        batch.getItems,
        host,
        forest,
        databaseClient.getPort,
        databaseClient.getDatabase,
        securityContext.getUser,
        securityContext.getPassword,
        timestamp))
      logInfo(f"Sucessfully Added Partition" + batch.getJobBatchNumber)
    }
  }

  class queryFailed extends QueryFailureListener {
    override def processFailure(e: QueryBatchException): Unit = {
      logInfo(e.printStackTrace().toString)
    }
  }
}

