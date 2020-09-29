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

package io.github.bkosaraju.utils.aws

import io.github.bkosaraju.utils.common.Session
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder
import io.github.bkosaraju.utils.common.Session

trait Config extends Session{

  lazy val DEFAULT_REGION = "ap-southeast-2"
  lazy val credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance()
  lazy val INPROGRESS = Array("PENDING","RUNNING")
  lazy val FINISHED = Array("CANCEL_PENDING","COMPLETED","CANCELLED","FAILED","INTERRUPTED")
  lazy val JOB_WAIT_TO_FINISH = 7200   //jobWaitToFinish
  lazy val LOG_RETRY_COUNTER = 15 //LOG_RETRY_COUNTER                             //s3_log_retry_counter
  lazy val SPARK_DRIVER_CORES = 2  //spark.driver.cores
  lazy val SPARK_DRIVER_MEMORY = "512m" //spark.driver.memory
  lazy val SPARK_NUM_EXECUTORS = 2 //spark.executor.instances
  lazy val SPARK_EXECUTOR_MEMORY = "512m" //spark.executor.memory
  lazy val SPARK_EXECUTOR_CORES = "2" //spark.executor.cores
  lazy val SPARK_SQL_SHUFFLE_PARTITIONS = 60 //spark.sql.shuffle.partitions
  lazy val SPARK_MASTER = "yarn" //spark.master
  lazy val SPARK_DEPLOY_MODE = "cluster" //spark.deploymode
  lazy val DEFAULT_APP_BUCKET = "config" //appBucketName
}
