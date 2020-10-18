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

package io.github.bkosaraju.utils.aws.emr.functions

import io.github.bkosaraju.utils.aws.{AWSClientConfigBuilder, AwsUtils, Config}
import io.github.bkosaraju.utils.common.Exceptions
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder
import com.amazonaws.services.elasticmapreduce.model.DescribeClusterRequest
import com.amazonaws.services.s3.model.S3ObjectSummary

class ExtractEMRSparkStepLog(config :Map[String,String]) extends Config with Exceptions {
  val utils = new AwsUtils
  var retryCounter, cntr = config.getOrElse("s3_log_retry_counter", LOG_RETRY_COUNTER).toString.toInt
  logger.info("Extracting the logs from application service")
  var logPaths: List[S3ObjectSummary] = List[S3ObjectSummary]()
  var loopBreaker = false

  def extractLogs(): Unit = {
    try {
      val clusterRequest = new DescribeClusterRequest
      clusterRequest.withClusterId(config("emrClusterId"))
      val emr = AmazonElasticMapReduceClientBuilder
        .standard()
        .withCredentials(credentialsProvider)
        .withClientConfiguration(AWSClientConfigBuilder(config))
        .withRegion(config.getOrElse("regionName", DEFAULT_REGION))
        .build()
      val logBasePath = emr.describeCluster(clusterRequest)
        .getCluster
        .getLogUri
        .replaceAll("^s3[a,n]:", "s3:") + config("emrClusterId") + "/steps/" + config("stepId") + "/"
      logPaths = utils.listS3Objects(logBasePath)
      if (logPaths.isEmpty && retryCounter > 0) {
        logger.info(s"Apparently logs not copied to S3 yet, hence waiting for another 30 seconds ..(${retryCounter} of ${cntr})")
        Thread.sleep(30000L)
        retryCounter -= 1
        extractLogs()
      } else if (logPaths.isEmpty) {
        throw UnableToDownloadLogs(s"Unable to find logs in given URI :${logBasePath}")
      } else if( logPaths.nonEmpty && ! loopBreaker ){
        logger.info(s"found following Objects in step log location(${logPaths.map(_.getBucketName).head}: ${logPaths.map(_.getKey).toList.mkString(",")} ")
        logPaths.foreach(objSummary => {
          logger.info("==============================================================================================")
          logger.info(s"Log for : ${objSummary.getKey}")
          logger.info(utils.getS3ObjectAsString(
            config ++ Map("bucketName" -> objSummary.getBucketName),
            objSummary.getKey
          ))
          logger.info("==============================================================================================")
        })
        loopBreaker = true
      }
    } catch {
      case e: Exception => {
        logger.error("Unable to get the log location", e)
        throw e
      }
    }
  }
}


object ExtractEMRSparkStepLog {
  def apply(config: Map[String,String]): Unit = {
    val logExtractor = new ExtractEMRSparkStepLog(config)
    logExtractor.extractLogs()
  }
}

