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

package io.github.bkosaraju.utils.aws.emr

import java.util
import scala.collection.JavaConverters._
import io.github.bkosaraju.utils.aws.Config
import io.github.bkosaraju.utils.aws.emr.functions.{ConfigMigrator, ExtractEMRSparkStepLog, GetClusterDetails, SparkCommandBuilder}
import io.github.bkosaraju.utils.common.Exceptions
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder
import com.amazonaws.services.elasticmapreduce.model.{AddJobFlowStepsRequest, DescribeClusterRequest, HadoopJarStepConfig, ListStepsRequest, StepConfig}
import com.amazonaws.services.elasticmapreduce.util.StepFactory

class RunSparkJobInEMR extends Config with Exceptions{

  def runSparkJob(config: Map[String,String]): Unit = {
    logger.info("commencing spark-app as EMR step..")
    val appConfig = collection.mutable.Map[String, String]() ++ config

    val sparkApplicationName : String = (config.getOrElse(
        "sparkAppName",
      config.getOrElse("jobName","spark-app")+"-"+ config.getOrElse("taskName","taskExecutionId"))+ "-" + config("taskExecutionId")
      ).slice(0,60).toLowerCase
      .replaceAll("_","-")
    appConfig.put("sparkApplicationName",sparkApplicationName)
    appConfig.put("emrClusterId",GetClusterDetails(appConfig.toMap))
    val stepFactory = new StepFactory()
    val emr = AmazonElasticMapReduceClientBuilder
      .standard()
      .withCredentials(credentialsProvider)
      .withRegion(config.getOrElse("regionName",DEFAULT_REGION))
      .build()

    val jobRequest = new AddJobFlowStepsRequest()
    jobRequest.setJobFlowId(appConfig("emrClusterId"))

    val stepConfig = new util.ArrayList[StepConfig]()

    val sparkStepInfo = SparkCommandBuilder(appConfig.toMap)
    val sparkStep = new StepConfig()
      .withName(sparkApplicationName)
      .withActionOnFailure("CONTINUE")
      .withHadoopJarStep(sparkStepInfo)

    stepConfig.add(sparkStep)
    jobRequest.withSteps(stepConfig)
    val res = emr.addJobFlowSteps(jobRequest)

    val stepsRequest = (new ListStepsRequest).withClusterId(appConfig("emrClusterId")).withStepIds(res.getStepIds)
    var jobStatus = ""
    var waitToFinishDuration, finishDuration = config.getOrElse("jobWaitToFinish", JOB_WAIT_TO_FINISH).toString.toLong
    var completionFlag = false
    try {
    while (!(waitToFinishDuration == 0 || completionFlag)) {
      waitToFinishDuration -= 5
      if (waitToFinishDuration % 120 == 0) {
        logger.info(s"polling job and waiting to be completed( current reported status : ${jobStatus} ), remaining time(${waitToFinishDuration / 60} / ${finishDuration / 60 } minutes ) ")
      }
      jobStatus = emr.listSteps(stepsRequest)
        .getSteps
        .asScala
        .map(x =>x.getStatus.getState)
        .head
      if (FINISHED.contains(jobStatus)) {
        completionFlag = true
      } else {
        Thread.sleep(5000)
      }
    }

    if (waitToFinishDuration <= 0 && !completionFlag ) {
      throw TooLongToCompleteJob(s"job not completed in specified duration (${finishDuration}) hence terminating application with error")
    }else if (jobStatus=="COMPLETED") {
      logger.info("spark application completed successfully!!")
    } else {
      throw NonZeroReturnCodeError(s"spark application terminated with non-zero status(${jobStatus}) ..")
    }
    } catch {
      case e : Exception => {
        logger.error("Error occurred while running job...",e)
        throw e
      }
    } finally {
      (new ConfigMigrator(appConfig.toMap)).deleteConfig
      logger.info(s"Job Timeline Info : \n${
        emr.listSteps(stepsRequest)
          .getSteps
          .asScala
          .head
          .getStatus
          .getTimeline
          .toString.
          replaceAll("\\{|\\}","")
          .replaceAll(",","\n")}"
      )
      ExtractEMRSparkStepLog(
        (appConfig ++ Map("stepId" -> stepsRequest.getStepIds.asScala.head)).toMap
      )
    }
  }

}

object RunSparkJobInEMR {
  def apply(config : Map[String,String]): Unit = {
    (new RunSparkJobInEMR()).runSparkJob(config)
  }
}
