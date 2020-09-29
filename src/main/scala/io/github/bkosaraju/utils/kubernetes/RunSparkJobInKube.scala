/*
 *   Copyright (C) 2019-2020 bkosaraju
 *   All Rights Reserved.
 *
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied.  See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package io.github.bkosaraju.utils.kubernetes

import java.util.UUID

import io.github.bkosaraju.utils.common.Exceptions
import io.github.bkosaraju.utils.kubernetes.model.Job
import io.github.bkosaraju.utils.common.Exceptions
import io.kubernetes.client.openapi.ApiException
import io.kubernetes.client.openapi.apis.{BatchV1Api, CoreV1Api}

import collection.JavaConverters._

class RunSparkJobInKube(jobConfiguration: Map[String,String])  extends Config with Exceptions {

  def runJob(): Unit = {
    val tempRunId = UUID
      .randomUUID()
      .toString
      .replaceAll("-", "")

    val jc = collection.mutable.Map[String,String]() ++ jobConfiguration

    val execIdPrepender =
    if (jc.contains("taskExecutionId") && jc.contains("jobExecutionId")) {
      s"""${jc("jobExecutionId")}-${jc("taskExecutionId")}"""} else {""}

    val runPreprender =
    if (jc.contains("taskId") && jc.contains("jobId")) {
      s"""${jc("jobId")}-${jc("taskId")}"""} else {""}


    val pipelinePrepender = Seq(runPreprender,execIdPrepender).filter(_.nonEmpty).mkString("-")

    if (pipelinePrepender.nonEmpty) {
      jc.put("pipelinePrepender", "SparkApp-" + pipelinePrepender) + "-"
    }

val providedJobName: String =
      (jobConfiguration.getOrElse("sparkAppName",
          jc.getOrElse("pipelinePrepender",
          "app-" + SPARK_CONTAINER_NAME)
      ) + "-" + tempRunId
    ).slice(0,40).toLowerCase.replaceAll("_","-")

    logger.info(s"Constructed Application Pod Names to : ${providedJobName}" )
    val nameSpace = jobConfiguration.getOrElse("namespace", DEFAULT_NAMESPACE)
    var completionFlag: Boolean = false
    val config = collection.mutable.Map[String, String]() ++ jobConfiguration
    config.put("tempRunId",tempRunId)
    config.put("sparkContainerName",providedJobName)
    var kubeClient = KubeClient(config.toMap) //.setDebugging(true)
    var api = new CoreV1Api(kubeClient)
    var failFlag = false
    var batchApi = new BatchV1Api(kubeClient)
    try {
      logger.info("commencing spark application launch in kubernetes")
      var waitToFinishDuration, finishDuration = config.getOrElse("jobWaitToFinish", JOB_WAIT_TO_FINISH).toString.toLong
      val jobConfig = Job(config.toMap)
      val batchJob = batchApi.createNamespacedJob(nameSpace, jobConfig, null, null, null)
      val jobName = batchJob.getMetadata.getName
      logger.info(s"submitted spark job ${jobName} and waiting for completion..")
      while (!(waitToFinishDuration == 0 || completionFlag)) {
        waitToFinishDuration -= 5
        if (waitToFinishDuration % 120 == 0) {
          logger.info(s"polling job and waiting to be completed, remaining time(${waitToFinishDuration / 60} / ${finishDuration / 60 } minutes ) ")
        }
        if (waitToFinishDuration % 600 == 0) {
          //Refresh token as that might be expired
          logger.info("re-initialing client to renew new token")
          kubeClient = KubeClient(config.toMap) //.setDebugging(true)
          api = new CoreV1Api(kubeClient)
          batchApi = new BatchV1Api(kubeClient)
        }
        val jobStatus = batchApi.readNamespacedJob(jobName, nameSpace, "true", true, null)
        val failCount : Int = if (jobStatus.getStatus.getFailed == null ) 0 else jobStatus.getStatus.getFailed
        failFlag = failCount >= jobStatus.getSpec.getBackoffLimit
        if (jobStatus.getStatus.getCompletionTime != null || failFlag  ){
          completionFlag = true
        } else {
          Thread.sleep(5000)
        }
      }

      if (waitToFinishDuration <= 0) {
        throw TooLongToCompleteJob(s"job Not completed in specified duration (${finishDuration}) hence terminating application with error")
      } else if (! failFlag && api.readNamespacedPod(jobName+"-driver",nameSpace,null,true,null).getStatus.getPhase.equalsIgnoreCase("Succeeded")) {
        logger.info("Spark Application Completed Successfully!!")
      } else {
        throw NonZeroReturnCodeError(s"spark application terminated with failed status ..")
      }
    } catch {
      case apiEx: ApiException => {
        logger.error("Unknown Error occurred while running job...",apiEx)
        apiEx.getResponseBody
        throw apiEx
      }
      case e: Exception => {
        logger.error("Error occurred while running job...",e)
        throw e
      }
    }
    finally {
      new ConfigMigrator(config.toMap).deleteConfig
      val podList = api.listNamespacedPod(nameSpace,null,null,null,null,null,null,null,null,null).getItems.asScala
      podList.map(pod => pod.getMetadata.getName)
        .filter(pod => pod.matches(s"${providedJobName}.*")).foreach(pod => {
        logger.info("==============================================================================================")
        logger.info(s"Log for pod : ${pod}")
        UploadPodLog(config.toMap,pod,nameSpace,api)
        ExtractPodLog(pod,nameSpace,api)
        logger.info("==============================================================================================")
        logger.info(s"trying to delete pod ${pod} from cluster..")
        DeleteConfig().deletePod(pod,nameSpace,api)
      })
      DeleteConfig().deleteJob(providedJobName,nameSpace,batchApi)
      }
    }
}

object RunSparkJobInKube {
  def apply(config: Map[String, String]): Unit = new RunSparkJobInKube(config).runJob()
}
