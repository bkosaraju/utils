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

package io.github.bkosaraju.utils.kubernetes.model

import io.github.bkosaraju.utils.common.Exceptions
import io.github.bkosaraju.utils.kubernetes.{Config, ConfigMigrator, EKSCluster}
import io.github.bkosaraju.utils.common.Exceptions

import collection.JavaConverters._

class SparkCommandBuilder(config: Map[String,String]) extends Config with Exceptions {
  //Mandatory Extra properties className,applicationJar, additionalJars(optional)
  def generateSparkCommand : java.util.List[String] = {
    try {
    var cmd = collection.mutable.ArrayBuffer[String]()
    val sparkConf  = collection.mutable.Map[String,String]() ++ ConfigMigrator(config)
    sparkConf.put("spark.kubernetes.container.image",config.getOrElse("spark.kubernetes.container.image",config.getOrElse("sparkContainerImage",SPARK_CONTAINER_IMAGE)))
    sparkConf.put("spark.kubernetes.namespace",config.getOrElse("spark.kubernetes.namespace",config.getOrElse("namespace", DEFAULT_NAMESPACE)))
    sparkConf.put("spark.kubernetes.authenticate.driver.serviceAccountName",config.getOrElse("podServiceAccount",POD_SERVICE_ACCOUNT))
    sparkConf.put("spark.kubernetes.driver.volumes.persistentVolumeClaim.appcodevolume.mount.path",config.getOrElse("volumeMountPath",VOLUME_MOUNT_PATH))
    sparkConf.put("spark.kubernetes.driver.volumes.persistentVolumeClaim.appcodevolume.mount.subPath",config.getOrElse("volumeMountPath",VOLUME_MOUNT_PATH))
    sparkConf.put("spark.kubernetes.driver.volumes.persistentVolumeClaim.appcodevolume.mount.readOnly","true")
    sparkConf.put("spark.kubernetes.driver.volumes.persistentVolumeClaim.appcodevolume.options.claimName",config.getOrElse("persistentVolumeClaimVolumeName",PERSISTENT_VOLUME_CLAIM_VOLUME_NAME))
    sparkConf.put("spark.kubernetes.driver.pod.name",config.getOrElse("spark.kubernetes.driver.pod.name",config("sparkContainerName")+"-driver"))
    sparkConf.put("spark.kubernetes.executor.request.cores",config.getOrElse("spark.kubernetes.executor.request.cores",KUBE_EXECUTOR_REQUEST_CORES))
    sparkConf.put("spark.executor.instances",config.getOrElse("spark.executor.instances",SPARK_NUM_EXECUTORS.toString))
    sparkConf.put("spark.executor.memory",config.getOrElse("spark.executor.memory",SPARK_EXECUTOR_MEMORY))
    sparkConf.put("spark.executor.cores",config.getOrElse("spark.executor.cores",SPARK_EXECUTOR_CORES.toString))
    sparkConf.put("spark.sql.shuffle.partitions",config.getOrElse("spark.sql.shuffle.partitions",SPARK_SQL_SHUFFLE_PARTITIONS.toString))

    if (! sparkConf.contains("className") || ! sparkConf.contains("applicationJar") ) {
      throw InvalidArgumentsPassed("configuration values 'className' and applicationJar are mandatory arguments for spark jab")
    }
    val jarsOption = if (sparkConf.contains("additionalJars")) { s" --jars ${sparkConf("additionalJars")} " } else ""
    val sparkCommand = s"/opt/spark/bin/spark-submit " +
      s"--master k8s://${EKSCluster(config).getEndpoint} " +
      s"--deploy-mode cluster " +
      s"--name ${sparkConf("sparkContainerName").replaceAll("_","-")} --class ${sparkConf("className")} " +
      s"--conf spark.executor.instances=${sparkConf("spark.executor.instances")} " +
      s"--conf spark.executor.cores=${sparkConf("spark.executor.cores")} " +
      s"--conf spark.sql.shuffle.partitions=${sparkConf("spark.sql.shuffle.partitions")} " +
      s"--conf spark.executor.memory=${sparkConf("spark.executor.memory")} " +
      jarsOption +
      sparkConf.keySet.filter(_.matches("^spark.kubernetes..*")).map( itm => s" --conf ${itm}=${sparkConf(itm)} ").mkString(" ") +
      sparkConf("applicationJar")  + " " + sparkConf("sparkAppProperties")

    cmd.append("/bin/sh")
    cmd.append("-c")
    cmd.append(sparkCommand)
    cmd.asJava
  } catch  {
      case e : Exception => {
        logger.error("Unable to build spark command to run the job!!")
        throw e
      }
    }
  }
}

object SparkCommandBuilder {
  def apply(config: Map[String, String]): java.util.List[String] = new SparkCommandBuilder(config).generateSparkCommand
}

