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
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyOrder
import scala.beans.{BeanProperty, BooleanBeanProperty}
//remove if not needed
import scala.collection.JavaConversions._

@SerialVersionUID(8544110366910266195L)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder(Array("job_id", "run_id", "number_in_job", "state", "task", "cluster_spec", "cluster_instance", "overriding_parameters", "start_time", "setup_duration", "execution_duration", "cleanup_duration", "trigger"))
class ResponseRunId extends Serializable {

  @JsonProperty("job_id")
  @BeanProperty
  var jobId: java.lang.Long = _

  @JsonProperty("run_id")
  @BeanProperty
  var runId: java.lang.Long = _

  @JsonProperty("number_in_job")
  @BeanProperty
  var numberInJob: java.lang.Long = _

  @JsonProperty("state")
  @BeanProperty
  var state: State = _

  @JsonProperty("task")
  @BeanProperty
  var task: Task = _

  @JsonProperty("cluster_spec")
  @BeanProperty
  var clusterSpec: ClusterSpec = _

  @JsonProperty("cluster_instance")
  @BeanProperty
  var clusterInstance: ClusterInstance = _

  @JsonProperty("overriding_parameters")
  @BeanProperty
  var overridingParameters: OverridingParameters = _

  @JsonProperty("start_time")
  @BeanProperty
  var startTime: java.lang.Long = _

  @JsonProperty("setup_duration")
  @BeanProperty
  var setupDuration: java.lang.Long = _

  @JsonProperty("execution_duration")
  @BeanProperty
  var executionDuration: java.lang.Long = _

  @JsonProperty("cleanup_duration")
  @BeanProperty
  var cleanupDuration: java.lang.Long = _

  @JsonProperty("trigger")
  @BeanProperty
  var trigger: String = _
}