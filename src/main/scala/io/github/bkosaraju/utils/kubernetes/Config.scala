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

import io.github.bkosaraju.utils.common.Session
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import io.github.bkosaraju.utils.common.Session

trait Config extends Session {
  lazy val credentialsProvider: DefaultAWSCredentialsProviderChain = DefaultAWSCredentialsProviderChain.getInstance()
  lazy val DEFAULT_REGION = Regions.AP_SOUTHEAST_2
  lazy val DEFAULT_EKS_CLUSTER_NAME = "eks-dm-dev" //eksClusterName
  lazy val REQUEST_SERVICE_NAME = "sts"
  lazy val DEFAULT_NAMESPACE = "sparkbatch" //namespace
  lazy val DEFAULT_APP_BUCKET = "config" //appBucketName
  lazy val APPLICATION_IAM_ROLE = "arn:aws:iam::xxxxxxxxx:role/xxxxxxxxxxxxx" //applicationIamRole
  lazy val SPARK_VERSION = "2.4.5"
  lazy val SPARK_CONTAINER_NAME: String = "spark" + "-" + SPARK_VERSION //sparkContainerName
  lazy val SPARK_CONTAINER_IMAGE = "xxxxxxxxxxxxxxx/spark:2.4.5" //sparkContainerImage
  lazy val PERSISTENT_VOLUME_ENABLED="false"  //persistentVolumeEnabled
  lazy val VOLUME_MOUNT_NAME = "persistent-storage" //volumeMountName
  lazy val VOLUME_MOUNT_PATH = "/data" //volumeMountPath
  lazy val PERSISTENT_VOLUME_CLAIM_VOLUME_NAME = DEFAULT_NAMESPACE+"-efs-claim" //persistentVolumeClaimVolumeName
  lazy val POD_SERVICE_ACCOUNT = "spark" //podServiceAccount
  lazy val SECURITY_CONTEXT_FSGROUP = 1337 //securityContextFsGroup
  lazy val CONTAINER_RETRY_LIMIT = 3 //backoffLimit
  lazy val KUBE_API_VERSION = "batch/v1" //apiVersion
  lazy val KUBE_POD_KIND = "Job" //podKind
  lazy val KUBE_EXECUTOR_REQUEST_CORES = "350m" //spark.kubernetes.executor.request.cores=350m
  lazy val KUBE_TOKEN_EXPIRY_DURATION = 60 //kubeTokenExpiryDuration
  lazy val SPARK_NUM_EXECUTORS = 2 //spark.executor.instances
  lazy val SPARK_EXECUTOR_MEMORY = "512m" //spark.executor.memory
  lazy val SPARK_EXECUTOR_CORES = "2" //spark.executor.cores
  lazy val SPARK_SQL_SHUFFLE_PARTITIONS = 60 //spark.sql.shuffle.partitions
  lazy val JOB_WAIT_TO_FINISH = 7200   //jobWaitToFinish

  lazy val LOG_S3_DESTINATION =  "s3://eks-logs/dataengineering/sprak"  ////s3LogDestination

  //Mandatory job Extra properties className,applicationJar, additionalJars(optional)

}
