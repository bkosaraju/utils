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

package io.github.bkosaraju.utils.databricks.model

import java.io.Serializable
import java.util.HashMap
import java.util.Map
import com.fasterxml.jackson.annotation.JsonAnyGetter
import com.fasterxml.jackson.annotation.JsonAnySetter
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyOrder
import scala.beans.{BeanProperty, BooleanBeanProperty}
//remove if not needed
import scala.collection.JavaConversions._

@SerialVersionUID(264350878016829158L)
@JsonInclude(JsonInclude.Include.NON_NULL)
class NewCluster extends Serializable {
  
  @BeanProperty
  @JsonProperty("spark_version")
  var sparkVersion: String = _

  @BeanProperty
  @JsonProperty("aws_attributes")
  var awsAttributes: AwsAttributes = _

  @BeanProperty
  @JsonProperty("node_type_id")
  var nodeTypeId: String = _

  @BeanProperty
  @JsonProperty("num_workers")
  var numWorkers: String = _

  @BeanProperty
  @JsonProperty("autoscale")
  var autoScale: AutoScale = _

  @BeanProperty
  @JsonProperty("cluster_name")
  var clusterName: String = _

  @BeanProperty
  @JsonProperty("spark_conf")
  var sparkConf: Map[String, String] = _

  @BeanProperty
  @JsonProperty("driver_node_type_id")
  var driverNodeTypeId: String = _

  @BeanProperty
  @JsonProperty("ssh_public_keys")
  var sshPublicKeys: Array[String] = _

  @BeanProperty
  @JsonProperty("custom_tags")
  var customTags: Map[String, String] = _

  @BeanProperty
  @JsonProperty("cluster_log_conf")
  var clusterLogConf: ClusterLogConf = _

  @BeanProperty
  @JsonProperty("spark_env_vars")
  var sparkEnvVars: Map[String, String] = _

  @BeanProperty
  @JsonProperty("autotermination_minutes")
  var autoTerminationMinutes: String = _

  @BeanProperty
  @JsonProperty("enable_elastic_disk")
  var enableElasticDisk: String = _

  @BeanProperty
  @JsonProperty("artifact_paths")
  var artifactPaths: Array[String] = _

  @BeanProperty
  @JsonProperty("instance_pool_id")
  var instancePoolId: String = _
  
}