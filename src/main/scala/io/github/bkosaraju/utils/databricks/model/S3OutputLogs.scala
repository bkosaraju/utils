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

import com.fasterxml.jackson.annotation.{JsonInclude, JsonProperty}
import scala.beans.BeanProperty

@SerialVersionUID(-6075630774842013296L)
@JsonInclude(JsonInclude.Include.NON_NULL)
class S3OutputLogs extends Serializable {

  @BeanProperty
  @JsonProperty("driver_log")
  var driverLog: String = _

  @BeanProperty
  @JsonProperty("driver_stdout")
  var driverstdOut: String = _

  @BeanProperty
  @JsonProperty("driver_stderr")
  var driverstderr: String = _


}

object S3OutputLogs {
  def apply(driverLog: String, driverStedOut : String, driverErr : String): S3OutputLogs = {
    val s3LogsDir = new S3OutputLogs()
    s3LogsDir.setDriverLog(driverLog)
    s3LogsDir.setDriverstderr(driverErr)
    s3LogsDir.setDriverstdOut(driverStedOut)
    s3LogsDir
  }
}
