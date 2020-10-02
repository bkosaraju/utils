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

package io.github.bkosaraju.utils.common

import java.io.{File, StringReader, StringWriter}

//import org.fusesource.scalate._
import java.nio.file.{Files, Path}

import io.github.bkosaraju.utils.aws.AwsUtils
import com.github.mustachejava.DefaultMustacheFactory
import org.apache.commons.io.FileUtils.copyFile

import scala.collection.JavaConverters.mapAsJavaMapConverter
//import au.gov.wsv.imf.utils.spark.Context
import com.amazonaws.services.s3.AmazonS3URI
import org.apache.commons.io.FileUtils

trait SQLTemplateWrapper
  extends Session {
  //with Context {
  /**
   * Templating Engine to Convert the SQL template into Spark Consumable SQL
   *
   * @param config   - Properties which shall be replaced into the template
   * @return SQL String with template substituted version.
   *         {{{
   *                val templateFileLocation = "/tmp/template/" + UUID.randomUUID().toString
   *                val templateFile = templateFileLocation + "/sqlFlie.template"
   *                hadoopfs.copyToLocalFile(new Path(props.getProperty("sqlFile")), new Path(templateFile))
   *                val engine = new TemplateEngine
   *                engine.workingDirectory = new File(templateFileLocation)
   *                val src = engine.source(templateFile, templateType)
   *                val sqlText = engine.layout(src, getMapfromProperties(props))
   *                engine.shutdown()
   *                Thread.sleep(1000)
   *                FileUtils.forceDelete(new File(templateFileLocation))
   *                return sqlText
   *         }}}
   */
  def getSQLFromTemplate( config : Map[String,String] ): String = {
    val templateFileLocation =  Files.createTempDirectory("")
    try {

      val templateFile = templateFileLocation + "/sqlFile.mustache"
      val inputSQLFile = config("sqlFile")
      if (inputSQLFile.toLowerCase.toLowerCase.matches("s3[a]?:.*")) {
        val S3URI = new AmazonS3URI(
          inputSQLFile.replaceAll("[sS]3[aA]://","s3://")
        )
        val templateConfig = collection.mutable.Map[String,String]() ++ config
        templateConfig.put("bucketName",S3URI.getBucket)
        (new AwsUtils).getS3Object(templateConfig.toMap,templateFile,S3URI.getKey)
      } else {
        copyFile(new File(config("sqlFile")),new File(templateFile))
      }
      //Disabled to avoid hadoop dependency
      //      else {
      //        hadoopfs.copyToLocalFile(new Path(config("sqlFile")), new Path(templateFile))
      //      }


      //      val engine = new TemplateEngine
      //      engine.workingDirectory = templateFileLocation.toFile
      //      val src = engine.source(templateFile, templateType)
      //      val sqlText = engine.layout(src, config)
      //      engine.shutdown()

      val mustacheFactory = new DefaultMustacheFactory()
      val mustache = mustacheFactory.compile(new StringReader(FileUtils.readFileToString(new File(templateFile))),"template.file")
      val stringWriter = new StringWriter()
      mustache.execute(stringWriter,config.asJava).flush()
      Thread.sleep(1000)
      FileUtils.deleteDirectory(templateFileLocation.toFile)
      stringWriter.toString
    } catch {
      case e: Throwable => {
        FileUtils.deleteDirectory(templateFileLocation.toFile)
        logger.error("Unable to Generate the Template From Given Input File : "+config.getOrElse("sqlFile","Not Provided"))
        throw e
      }
    } finally {
      FileUtils.deleteDirectory(templateFileLocation.toFile)
    }
  }
}