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
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder
import collection.JavaConverters._

class GetClusterDetails extends Config with Exceptions {
  def getClusterIdByName(config: Map[String,String]): String = {
    val emr = AmazonElasticMapReduceClientBuilder
      .standard()
      .withCredentials(credentialsProvider)
      .withRegion(config.getOrElse("regionName", DEFAULT_REGION))
      .build()

    val activeClusters = emr
      .listClusters
      .getClusters
      .asScala
      .filter(_.getName.equalsIgnoreCase(config("emrClusterName")))
      .filter(!_.getStatus.getState.contains("TERMINATED"))
    if(activeClusters.nonEmpty) {
      activeClusters.head.getId
      } else{
      throw new NonExistedEMRCluster(s"No cluster running with name ${config("emrClusterName")}")
    }
  }

  def validateEMRClusterById(config: Map[String,String] ) : Boolean =  {
    val emr = AmazonElasticMapReduceClientBuilder
      .standard()
      .withCredentials(credentialsProvider)
      .withRegion(config.getOrElse("regionName", DEFAULT_REGION))
      .build()
    val activeClusters = emr
      .listClusters
      .getClusters
      .asScala
      .filter(_.getId.equalsIgnoreCase(config("emrClusterId")))
      .filter(!_.getStatus.getState.contains("TERMINATED"))
    if(activeClusters.nonEmpty) true else false
  }

  def getActiveCluster (config: Map[String,String] ) : String = {

    if(config.contains("emrClusterId") && validateEMRClusterById(config) ) {
        config("emrClusterId")
      } else if (config.contains("emrClusterName")) {
        getClusterIdByName(config)
      } else {
        throw new InvalidArgumentsPassed("must provide valid emrClusterName/emrClusterId or Cluster is in terminated state for provided jobs ..")
      }
  }
}

object GetClusterDetails {
  def apply(config: Map[String,String]): String =
    (new GetClusterDetails()).getActiveCluster(config)
}