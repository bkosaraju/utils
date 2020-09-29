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

package io.github.bkosaraju.utils.databricks.sdk

import io.github.bkosaraju.utils.databricks.model._
import org.springframework.http.converter.HttpMessageConverter
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter
import org.springframework.http.{HttpEntity, HttpHeaders, HttpMethod, MediaType}
import org.springframework.web.client.RestTemplate

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
class Utils(config : Map[String,String]) extends Config {

  def getTemplateforJson : RestTemplate = {
    val restTemplate = new RestTemplate
    val messageConverter = ArrayBuffer[HttpMessageConverter[_]]()
    val converter = new MappingJackson2HttpMessageConverter()

    converter.setSupportedMediaTypes(
    MediaType.parseMediaTypes(config.getOrElse("mediaType",MediaType.ALL_VALUE))
    )
    messageConverter.append(converter.asInstanceOf[HttpMessageConverter[_]])
    restTemplate.setMessageConverters(messageConverter.asJava)
    restTemplate
  }

  def getAuthHeader : HttpHeaders = {
    val headers = new HttpHeaders()
    headers.setContentType(MediaType.APPLICATION_JSON)
    if (config.contains("password_token")) {
      headers.set("Authorization", "Bearer " + config("password_token"))
    } else {
      throw new UnknownError("password_token parameter must be provided connect to API")
    }
    headers
  }

  def getNewClusterJobDef :  RunSubmit = {
    val runSubmit = new RunSubmit
    val autoScale = new AutoScale
    val newCluster = new NewCluster
    val clusterLogConf = new ClusterLogConf
    val awsAttributes = new AwsAttributes
    val sparkSubmitTask = new SparkSubmitTask
    val s3logConf = new S3StorageInfo

    runSubmit.setRunName(config.getOrElse("run_name",RUN_NAME))

    autoScale.setMinWorkers(config.getOrElse("min_workers",MIN_WORKERS).toString.toInt)
    autoScale.setMaxWorkers(config.getOrElse("max_workers",MAX_WORKERS).toString.toInt)

    awsAttributes.setInstanceProfileArn(config.getOrElse("instance_profile_arn",INSTANCE_PROFILE_ARN))

    s3logConf.setDestination(config.getOrElse("s3_log_destination",LOG_S3_DESTINATION))
    s3logConf.setCannedAcl(config.getOrElse("s3_log_canned_acl",LOG_S3_CANNED_ACL))
    if(config.contains("s3_log_kms_key")) {
      s3logConf.setKmsKey(config("s3_log_kms_key"))
    }
    s3logConf.setEnableEncryption(config.getOrElse("s3_log_enable_encryption",LOG_S3_ENABLE_ENCRYPTION).toString.toBoolean)
    s3logConf.setRegion(config.getOrElse("s3_log_region",LOG_S3_REGION))
    clusterLogConf.setS3(s3logConf)

    newCluster.setSparkVersion(config.getOrElse("spark_version",SPARK_VERSION))
    newCluster.setAutoScale(autoScale)
    newCluster.setAwsAttributes(awsAttributes)
    newCluster.setInstancePoolId(config.getOrElse("instance_pool_id",INSTANCE_POOL_ID))
    newCluster.setClusterLogConf(clusterLogConf)


    sparkSubmitTask.setParameters(
      config.getOrElse("spark_submit_parameters","")
      .split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)")
      .toList
      .map(_.replaceAll(""""""","")).asJava
    )

    runSubmit.setNewCluster(newCluster)
    runSubmit.setSparkSubmitTask(sparkSubmitTask)
    runSubmit
  }



}
