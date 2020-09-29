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

import io.github.bkosaraju.utils.aws.Config
import io.github.bkosaraju.utils.common.Exceptions
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig
import collection.JavaConverters._

class SparkCommandBuilder(config: Map[String,String]) extends Config with Exceptions {
  //Mandatory Extra properties className,applicationJar, additionalJars(optional)
  def generateSparkCommand : HadoopJarStepConfig = {
    try {
      var cmd = collection.mutable.ArrayBuffer[String]()
      val sparkConf  = collection.mutable.Map[String,String]() ++ ConfigMigrator(config)

      sparkConf.put("spark.driver.cores",config.getOrElse("spark.driver.cores",SPARK_DRIVER_CORES.toString))
      sparkConf.put("spark.driver.memory",config.getOrElse("spark.driver.memory",SPARK_DRIVER_MEMORY))
      sparkConf.put("spark.executor.instances",config.getOrElse("spark.executor.instances",SPARK_NUM_EXECUTORS.toString))
      sparkConf.put("spark.executor.memory",config.getOrElse("spark.executor.memory",SPARK_EXECUTOR_MEMORY))
      sparkConf.put("spark.executor.cores",config.getOrElse("spark.executor.cores",SPARK_EXECUTOR_CORES.toString))
      sparkConf.put("spark.sql.shuffle.partitions",config.getOrElse("spark.sql.shuffle.partitions",SPARK_SQL_SHUFFLE_PARTITIONS.toString))

      if (! sparkConf.contains("className") || ! sparkConf.contains("applicationJar") ) {
        throw InvalidArgumentsPassed("configuration values 'className' and applicationJar are mandatory arguments for spark jab")
      }

      var sparkCommand = List[String]("spark-submit",
        "--master", config.getOrElse("spark.master",SPARK_MASTER) ,
        "--deploy-mode", config.getOrElse("spark.deploymode",SPARK_DEPLOY_MODE),
        "--conf", s"spark.executor.instances=${sparkConf("spark.executor.instances")}",
        "--conf", s"spark.executor.cores=${sparkConf("spark.executor.cores")}",
        "--conf", s"spark.sql.shuffle.partitions=${sparkConf("spark.sql.shuffle.partitions")}",
        "--conf", s"spark.executor.memory=${sparkConf("spark.executor.memory")}",
        "--class",s"${sparkConf("className")}",
        "--name", s"${sparkConf("sparkApplicationName")}"
      )
      if (sparkConf.contains("additionalJars") && sparkConf("additionalJars").nonEmpty) {
      sparkCommand ++= List("--jars",sparkConf("additionalJars"))
      }

        sparkConf.keySet.filter(_.matches("^spark\\..*")).foreach(itm =>
            sparkCommand ++= List("--conf", s"$itm=${sparkConf(itm)}")
        )
      sparkCommand ++= List(
        sparkConf("applicationJar"),
      sparkConf("sparkAppProperties")
    )
      new HadoopJarStepConfig()
        .withJar("command-runner.jar")
          .withArgs(sparkCommand.asJavaCollection)
    } catch  {
      case e : Exception => {
        logger.error("Unable to build spark command to run the job!!")
        throw e
      }
    }
  }
}

object SparkCommandBuilder {
  def apply(config: Map[String, String]): HadoopJarStepConfig = {
    new SparkCommandBuilder(config).generateSparkCommand
  }
}


