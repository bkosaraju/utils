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

trait Config {

  //Logs Specs
  lazy val LOG_S3_DESTINATION =  "s3://dbc-cluster-logs/dataengineering/"   //s3_log_destination
  lazy val LOG_S3_REGION = "ap-southeast-2"                                     //s3_log_region
  lazy val LOG_S3_ENABLE_ENCRYPTION = true                                     //s3_log_enable_encryption
  lazy val LOG_S3_CANNED_ACL = "bucket-owner-full-control"                      //s3_log_canned_acl
  lazy val LOG_S3_DRIVER_LOGNAME = "/log4j-active.log"
  lazy val LOG_S3_DRIVER_STDOUT = "/stdout"
  lazy val LOG_S3_DRIVER_STDERR = "/stderr"
  lazy val LOG_RETRY_COUNTER = 10 //LOG_RETRY_COUNTER                             //s3_log_retry_counter
  //s3_log_kms_key for log KMS Key

  lazy val SPARK_ENV_PYSPARK_PATH = "/databricks/python3/bin/python3"
  lazy val MIN_WORKERS = 1 //min_workers
  lazy val MAX_WORKERS = 4 //max_workers
  lazy val SPARK_VERSION = "5.5.x-scala2.11" //spark_version
  lazy val CLUSTER_NAME = "Data Engineering Cluster"
  lazy val RUN_NAME = "Data Engineering Job" //run_name

  //AWS Specs
  //lazy val NODE_TYPE_ID = "m4.2xlarge"
  //lazy val DRIVER_NODE_TYPE_ID = "m4.xlarge"
  //lazy val AWS_FIRST_ON_DEMAND = 1
  //lazy val AWS_AVAILABILITY = "SPOT_WITH_FALLBACK"
  //lazy val EBS_VOLUME_TYPE = "GENERAL_PURPOSE_SSD"
  //lazy val EBS_VOLUME_COUNT = 1
  //lazy val EBS_VOLUME_SIZE = 100
  //lazy val AWS_ZONE_ID = "ap-southeast-2a"
  lazy val INSTANCE_PROFILE_ARN = "arn:aws:iam::xxxxxxxxx:instance-profile/xxxxxxxxxxxxxxxxx" //instance_profile_arn
  lazy val SPOT_BID_PRICE_PERCENT = 100
  lazy val AUTOTERMINATION_MINUTES = 1
  //lazy val INSTANCE_POOL_ID = "1011-034506-foggy3-pool-byROg1zQ"
  lazy val INSTANCE_POOL_ID = "1017-033550-calf41-pool-Qpx92RHs" //instance_pool_id


  //DB Specific

  lazy val DB_URL = "https://xxxxxxxxxxxxxxxx/api" //DB_URL
  lazy val API_VERSION = "2.0" //API_VERSION
  lazy val JOBS_SUBMIT_ENDPOINT = "/jobs/runs/submit"
  lazy val JOB_RUN_GET_ENDPOINT = "/jobs/runs/get"


  //Runtime Options(custom parameters)
  lazy val JOB_WAIT_TO_FINISH = 3600   // jobWaitToFinish job wait time to finish for polling set jobWaitToFinish to overwrite
  lazy val JOB_MAX_RETRIES = 3        // max_retries number of times INTERNAL_ERROR from cluster can be tried set max_retries to overwrite

}
