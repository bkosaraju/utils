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

import java.io.{File, FileOutputStream}
import java.util.{Properties, UUID}

import io.github.bkosaraju.utils.aws.{AwsUtils, Config}
import org.apache.commons.io.FileUtils

import collection.JavaConverters._
import scala.collection.mutable

class ConfigMigrator (jobConfig: Map[String,String]) extends Config {
  val providedJobName: String = (jobConfig.getOrElse("sparkAppName",jobConfig.getOrElse("taskName","taskExecutionId"))+ "-" + jobConfig("taskExecutionId")).slice(0,30).toLowerCase.replaceAll("_","-")
  var taskExecutionConfig: mutable.Map[String, String] = collection.mutable.Map[String,String]() ++ jobConfig
  val tmpDir = new File(FileUtils.getTempDirectory + "/" + taskExecutionConfig("sparkApplicationName"))
  val s3ConfigKey = tmpDir.toString.replaceAll("^/\\w+/","tmp/")+ "/app.properties"
  def configMigrator: Map[String,String] = {
    try {
      val props = new Properties()
      props.putAll(taskExecutionConfig.asJava)
      FileUtils.forceMkdir(tmpDir)
      val configFile = new File(tmpDir + "/app.properties")
      val f = new FileOutputStream(configFile)
      props.store(f, null)
      f.close()
      val appConfig = collection.mutable.Map[String, String]()
      appConfig ++= taskExecutionConfig
      appConfig.put("appConfigKey",configFile.toString)
      appConfig.put("bucketName", appConfig.getOrElse("appBucketName", DEFAULT_APP_BUCKET))
      (new AwsUtils).putS3Object(appConfig.toMap, configFile.toPath.toString,  s3ConfigKey)
      FileUtils.deleteDirectory(tmpDir)
      val sparkAppProperties = "s3a://"+appConfig.getOrElse("appBucketName", DEFAULT_APP_BUCKET)+"/"+s3ConfigKey
      appConfig.put("sparkAppProperties",sparkAppProperties)
      appConfig.toMap
    } catch {
      case e: Exception =>
        logger.error("Unable to write config data into S3 for passing as job Config")
        FileUtils.deleteDirectory(tmpDir)
        throw e
    }
  }

  def deleteConfig : Unit = {
    try {
      logger.info("cleaning up temporary configuration directory from S3..")
      (new AwsUtils).deleteS3Object(taskExecutionConfig.getOrElse("appBucketName", DEFAULT_APP_BUCKET),  s3ConfigKey)
    }catch {
      case e : Exception => {
        logger.warn("Unable to delete temporary configuration directory from S3..",e)
        logger.debug("Error occurred while cleaning up configuration ",e)
      }
    }
  }

}

object ConfigMigrator {
  def apply(config: Map[String, String]): Map[String,String] = new ConfigMigrator(config).configMigrator
}
