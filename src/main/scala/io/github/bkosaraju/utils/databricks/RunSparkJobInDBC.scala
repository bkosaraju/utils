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

package io.github.bkosaraju.utils.databricks

import java.io.File
import java.util.{Properties, UUID}

import io.github.bkosaraju.utils.common.{Exceptions, Session}
import io.github.bkosaraju.utils.databricks.sdk.{Config, GetLogs, GetStatus, SubmitJob}
import org.apache.commons.io.FileUtils
import java.io.FileOutputStream

import io.github.bkosaraju.utils.aws.AwsUtils
import com.amazonaws.services.s3.AmazonS3URI
import io.github.bkosaraju.utils.common.{Exceptions, Session}
import org.slf4j.Logger

import collection.JavaConverters._
import scala.beans.BeanProperty

class RunSparkJobInDBC
  extends Config
    with Session
    with Exceptions {
  logger.info("preparing for RunSparkJob operation")
  @BeanProperty
  var jobConfig: Map[String, String] = _
  var completionFlag: Boolean = false

  override def logger: Logger = super.logger
  def runJob {
    logger.info("commencing spark application launch")
    var waitToFininshDuration, finishDuration = jobConfig.getOrElse("jobWaitToFinish", JOB_WAIT_TO_FINISH).toString.toLong
    var maxRetries, totalRetries =  jobConfig.getOrElse("max_retries", JOB_MAX_RETRIES).toString.toLong

    if (jobConfig.nonEmpty) {
      var runStatus = SubmitJob(jobConfig)
      logger.info("submitted spark application and waiting for completion..")
      while ( ! (waitToFininshDuration == 0 ||   completionFlag )) {
        waitToFininshDuration -= 5
        if (Seq("TERMINATED", "SKIPPED")
          .contains(GetStatus(jobConfig, runStatus).getState.getLifeCycleState)) {
          completionFlag = true
        } else if (GetStatus(jobConfig, runStatus).getState.getLifeCycleState=="INTERNAL_ERROR" && maxRetries > 0) {
          logger.warn(s"Internal error occurred from Databricks API, giving one more try(${totalRetries + 1 - maxRetries } of ${maxRetries} ) ")
          maxRetries -= 1
          runStatus = SubmitJob(jobConfig)
        } else if (GetStatus(jobConfig, runStatus).getState.getLifeCycleState=="INTERNAL_ERROR" && maxRetries == 0) {
          logger.error(s"Given enough${maxRetries} retries no more to try.. terminating")
          completionFlag = true
        } else {
          Thread.sleep(5000)
        }
      }
      if (waitToFininshDuration < 0) {
        throw TooLongToCompleteJob(s"job Not completed in specified duration (${finishDuration}) hence terminating application with error")
      } else {
        val jobStatus = GetStatus(jobConfig, runStatus).getState
        logger.info(s"""Job Lifecycle state reported as ${jobStatus.getLifeCycleState} and job status to ${jobStatus.getResultState} """)
        jobStatus.getLifeCycleState match {
          case "TERMINATED" => {
            logger.info(GetLogs(jobConfig, runStatus))
            jobStatus.getResultState match {
              case "SUCCESS" => {
                logger.info("spark application completed successfully...")
              }
              case _ =>{
                throw NonZeroReturnCodeError(s"spark application terminated with ${jobStatus.getResultState} status")
              }
            }
          }
          case _ => throw UnknownJobStateReported(
            s"""Job Lifecycle state reported as ${jobStatus.getLifeCycleState}
               |, job status: ${jobStatus.getResultState}
               | with state message : ${jobStatus.getStateMessage}""".stripMargin)
        }
      }
    }
  }

  def configMigrator: Map[String,String] = {
    val tempRunId = UUID
      .randomUUID()
      .toString
      .replaceAll("-", "")
    val tmpDir = new File(FileUtils.getTempDirectory + "/" + tempRunId)

    try {
      val props = new Properties()
      props.putAll(jobConfig.asJava)
      FileUtils.forceMkdir(tmpDir)
      val configFile = new File(tmpDir + "/app.properties")
      val f = new FileOutputStream(configFile)
      props.store(f, null)
      f.close()
      val appConfig = collection.mutable.Map[String, String]()
      appConfig ++= jobConfig

      appConfig.put("bucketName", appConfig.getOrElse("appBucketName", "config"))
      (new AwsUtils).putS3Object(appConfig.toMap, configFile.toPath.toString, tmpDir.toString.replaceAll("^/","") + "/app.properties")
      FileUtils.deleteDirectory(tmpDir)
      val sparkAppProperties = "s3a://"+appConfig.getOrElse("appBucketName", "config")  + tmpDir + "/app.properties"
      appConfig.put("spark_submit_parameters",appConfig.getOrElse("spark_submit_parameters","SampleApp")+"," + sparkAppProperties)
      appConfig.put("sparkAppProperties",sparkAppProperties)
      appConfig.toMap
    } catch {
      case e: Exception =>
        logger.error("Unable to write config data into S3 for passing as job Config")
        FileUtils.deleteDirectory(tmpDir)
        throw e
    }
  }


  def deleteConfig (config: Map[String,String]): Unit = {
    try {
      logger.info("cleaning up temporary configuration directory from S3..")
      val propsS3config = new AmazonS3URI(config("sparkAppProperties")
        .replaceAll("s3a:","s3:"))
      (new AwsUtils).deleteS3Object( propsS3config.getBucket, propsS3config.getKey)
    }catch {
      case e : Exception => {
        logger.warn("Unable to delete temporary configuration directory from S3..",e)
        logger.debug("Error occurred while cleaning up configuration ",e)
      }
    }
  }
}

object RunSparkJobInDBC extends Session {
  def apply(config: Map[String, String]): Unit = {
    val job = new RunSparkJobInDBC()
    job.setJobConfig(config)
    val sparkApplicationProperties = job.configMigrator
    try {
      job.setJobConfig(sparkApplicationProperties)
      job.runJob
    } catch {
      case e: Exception => {
        logger.error("Error occurred while running job..")
        job.deleteConfig(sparkApplicationProperties)
        throw e
      }
    } finally {
        job.deleteConfig(sparkApplicationProperties)
      }
    }
}
