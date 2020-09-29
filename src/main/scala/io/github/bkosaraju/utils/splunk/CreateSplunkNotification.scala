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

package io.github.bkosaraju.utils.splunk

import java.text.SimpleDateFormat
import java.util.TimeZone

import io.github.bkosaraju.utils.common.{Exceptions, MaskSensitiveValuesFromMap, Session}
import org.apache.camel.component.splunkhec.{SplunkHECComponent, SplunkHECConfiguration, SplunkHECEndpoint}
import org.apache.camel.impl.DefaultCamelContext
import org.apache.camel.component.splunkhec.SplunkHECProducer
import org.apache.camel.Exchange
import org.apache.camel.support.DefaultExchange
import org.apache.camel.support.DefaultMessage

import scala.collection.JavaConverters.asJavaIterableConverter
import scala.collection.JavaConversions.mapAsJavaMap

class CreateSplunkNotification
  extends Session
    with Exceptions {
  /**
   * Method to create Splunk notification with all the input parameters
   * @param config
   * mandatory arguments includes
   *  splunkUrl: Url for splunk ex: https://splunkURL:port
   *  splunkToken: auth token for splunk
   * Optional Arguments
   *  splunkSkipTlsVerify: defaulted to true in case if needed we need to set to false for security reasons
   *  splunkIndex: Index name for splunk
   *  splunkSource: Source of message
   *  splunkSourceType : Type of source
   *  Any argument starting with splunk get stripped and removed from message.
   */
  def createSplunkNotification(config: Map[String, String]): Unit = {
    try {
      if (config.getOrElse("splunkUrl", "").trim.isEmpty || config.getOrElse("splunkToken", "").trim.isEmpty) {
        logger.error("splunkUrl and splunkToken are mandatory arguments to pass")
        throw new InvalidArgumentsPassed("splunkUrl and splunkToken are mandatory arguments to pass")
      }

      val configuration = new SplunkHECConfiguration
      val camelContext = new DefaultCamelContext()
      configuration.setSkipTlsVerify(config.getOrElse("splunkSkipTlsVerify", "true").trim.toBoolean)
      configuration.setHost(java.net.InetAddress.getLocalHost.getHostName)

      //Commented as required by ITSS so it will be derived from token .
      configuration.setIndex(null)
      configuration.setSource(null)
      configuration.setSourceType(null)

//    configuration.setIndex(config.getOrElse("splunkIndex", "xxxxx"))
//    configuration.setSource(config.getOrElse("splunkSource", "xxxxxx"))
//    configuration.setSourceType(config.getOrElse("splunkSourceType", "xxxxxx"))

      //Message Specs
//      {
//        "header": {
//          "assignmentGroup": "Test Group",
//          "configurationItem": "Test Item",
//          "priority": "LOW",
//          "createTicket": "YES"
//        },
//        "body": {
//          "description": "FAILED - JOB_ID: 1004, Cluster Step: 101, On Cluster : BatchCluster (j-xxxxxx) at 2020-09-15 01:40:56",
//          "errorMessage": "unknown error",
//          "errorLogLocation": "s3://logs/emr/j-xxxxxx/101"
//        }
//      }


      val createTicket =
      if (Seq("yes","true").contains(config.getOrElse("splunkCreteTicket",config.getOrElse("createTicket","false")).toLowerCase())) {
        "true"
      } else { "false"}
      var header, derivedBody = collection.mutable.Map[String,String]()
      header.put("assignmentGroup",config.getOrElse("splunkAssignmentGroup",config.getOrElse("assignmentGroup","None")))
      header.put("configurationItem",config.getOrElse("splunkConfigurationItem",config.getOrElse("configurationItem","other")))
      header.put("priority",config.getOrElse("splunkPriority",config.getOrElse("priority","low")))
      header.put("createTicket",createTicket)

      //Body content
      val clusterName = {
        config.getOrElse("taskType","None") match {
          case  "spark_on_kubernetes" => s"""clusetr: ${config.getOrElse("eksClusterName","EKS Cluster")}"""
          case  "spark_on_emr" => s"""clusetr: ${config.getOrElse("emrClusterName","EMR Cluster")}"""
          case  "spark_on_databricks" => s"""clusetr: ${config.getOrElse("DB_URL","DBCKS Cluster")
            .replaceAll("^http[s]\\:\\/\\/","")
            .replaceAll("/.*","")}"""
        case _ => "Cluster"

        }
      }
      val df =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      df.setTimeZone(TimeZone.getTimeZone("Australia/Melbourne"))
      derivedBody.put("description",s"FAILED - JOB_ID: ${config.getOrElse("jobId","UKN")}, TaskId: ${config.getOrElse("taskId","UKN")}, On ${clusterName} at: ${df.format(java.util.Calendar.getInstance().getTime())}")
      val component = new SplunkHECComponent
      val url = config("splunkUrl").trim.replaceAll("/$", "") + "/" + config("splunkToken").trim
      val endpoint = new SplunkHECEndpoint(url, component, configuration)
      endpoint.setCamelContext(camelContext)

      val producer: SplunkHECProducer = new SplunkHECProducer(endpoint)
      producer.start()
      val camelExchange: Exchange = new DefaultExchange(camelContext)
      val message: DefaultMessage = new DefaultMessage(camelExchange)
      message.setBody(mapAsJavaMap(MaskSensitiveValuesFromMap(config ++ derivedBody).filter(!_._1.matches("^splunk.*"))))
      message.setHeaders(mapAsJavaMap(header))
      camelExchange.setIn(message)
      producer.process(camelExchange)
      producer.stop()
    } catch {
      case e : Exception => {
        logger.error("Unable to send event to splunk with given configurations")
        throw e
      }
    }
  }
}

object CreateSplunkNotification {
  def apply(config: Map[String,String]): Unit = {
    (new CreateSplunkNotification()).createSplunkNotification(config)
  }
}
