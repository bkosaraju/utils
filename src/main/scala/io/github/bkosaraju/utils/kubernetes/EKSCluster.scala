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

import java.util.{Calendar, Date}

import com.amazonaws.services.eks.{AmazonEKS, AmazonEKSClientBuilder}
import com.amazonaws.services.eks.model.{Cluster, DescribeClusterRequest}

class EKSCluster extends Config {

def getCluster(config: Map[String,String]) : Cluster = {
  val EKS: AmazonEKS =
    AmazonEKSClientBuilder
      .standard()
      .withCredentials(credentialsProvider)
      .withRegion(config.getOrElse("region",DEFAULT_REGION.getName))
      .build()

    EKS.describeCluster(
        new DescribeClusterRequest().withName(
          config.getOrElse("eksClusterName",DEFAULT_EKS_CLUSTER_NAME)
        )
      ).getCluster
  }
}

object EKSCluster {
  def apply(config: Map[String,String]): Cluster = {
    (new EKSCluster()).getCluster(config)

  }
}

