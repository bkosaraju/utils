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

package io.github.bkosaraju.utils.jsh

import java.io.{File, FileNotFoundException}
import java.nio.charset.StandardCharsets

import io.github.bkosaraju.utils.aws.AwsUtils
import io.github.bkosaraju.utils.common.{Exceptions, Session}
import com.jcraft.jsch._
import io.github.bkosaraju.utils.common
import io.github.bkosaraju.utils.common.Exceptions
import org.apache.commons.io.FileUtils

import collection.JavaConverters._

class SftpUtils
  extends common.Session
    with Exceptions{
  /**{{{
   * SFTP Parameter List:
   * sftpUser
   * sftpPrivateKey          - Optional (either key or password)
   * sftpPassword            - Optional (either key or password)
   * sftpHost
   * sftpPort                - Optional  defaulted to 22
   * sftpSourcePath
   * sftpTargetPath
   * sftpFilePattern         - defaulted to *
   * bucketName              - Only in case of target location is S3
   * proxyHost               - Optional
   * proxyPort               - Optional
   * proxyUser               - Optional (must provide proxyPassword if provided)
   * proxyPassword           - Optional
   * proxyType               - Optional (socks4 / socks[5] / http )
   * StrictHostKeyChecking   - Optional defaulted to no
   * PreferredAuthentications- Optional defaulted to publickey,password,keyboard-interactive
   * connectionTimeout       - Optional defaulted to 600000
   * purgeSourceFilesAfterPull - Optional defaulted to false
   * archiveSourceFilesAfterPull - Optional defaulted to false (when provided its mandatory to provide sourceFileArchivePath)
   * sourceFileArchivePath       - Optional (mandate to provide in case of archiveSourecFileAfterPull provided) - Strnig
   *}}}
   * @param config : Map[String,String]
   */
  //def fetchSftpFile(props: Properties) : Unit = {
  def fetchSftpFile(config: Map[String,String]) : Unit = {
    val ByteSize = 1024
    val tempDir = System.getProperty("java.io.tmpdir")+"/"+java.util.UUID.randomUUID().toString.replaceAll("-","") +"/"
    //new File(tempDir).mkdirs()
    FileUtils.forceMkdir(new File(tempDir))
    val jsch = new JSch()
    var tempFilename : String = ""
    var sftpClient :  ChannelSftp = null
    var session : com.jcraft.jsch.Session = null
    try {
      if (config.contains("sftpPrivateKey")) {
        logger.info("Authenticating with sftp PrivateKey..")
        if (config("sftpPrivateKey").length >1000) {
          jsch.addIdentity("id_rsa",
              config("sftpPrivateKey").getBytes(StandardCharsets.ISO_8859_1),
            null,
            null)
        } else {
          jsch.addIdentity(config("sftpPrivateKey"))
        }
      } else if (config.contains("sftpPassword")) {
        logger.info("Using Password to authenticate against source server..")
      } else {
        logger.error("must pass sftpPrivateKey or sftpPassword for authenticating against source server..")
        throw InvalidAuthenticationProvider("Unknown authentication method")
      }
      //Validate required archive arguments passed in case of archiving is enabled after pull..
      if (config.getOrElse("archiveSourceFilesAfterPull","false").equalsIgnoreCase("true") &&
        config.getOrElse("sourceFileArchivePath","").trim.isEmpty) {
        logger.error("It is mandatory to provide sourceFileArchivePath when you set to true..")
        throw new InvalidArgumentsPassed("It is mandatory to provide sourceFileArchivePath when you set archiveSourceFileAfterPull to true..")
      }

      session = jsch.getSession(
        config("sftpUser"),
        config("sftpHost"),
        config.getOrElse("sftpPort",22.toString).toInt
      )


      //setting default value to Strict host key checking
      session.setConfig("StrictHostKeyChecking", config.getOrElse("StrictHostKeyChecking","no"))
      session.setConfig("PreferredAuthentications", config.getOrElse("PreferredAuthentications","publickey,password,gssapi-with-mic,keyboard-interactive"))

      //Setting Up Proxy Configuration
      if (config.contains("proxyHost")){
        if (config.getOrElse("proxyType","http").equalsIgnoreCase("socks4")) {
          if (config.contains("ProxyPort")) {
            val proxy = new ProxySOCKS4(config("proxyHost"), config("proxyPort").toInt)
            if (config.contains("proxyUser")) {
              proxy.setUserPasswd(config("ProxyUser"),config("proxyPassword"))
            }
            session.setProxy(proxy)
          } else {
            session.setProxy(new ProxySOCKS4(config("proxyHost")))
          }
        } else if (config.getOrElse("proxyType","http").toLowerCase.contains("socks")) {
          if (config.contains("ProxyPort")) {
            val proxy = new ProxySOCKS5(config("proxyHost"), config("proxyPort").toInt)
            if (config.contains("proxyUser")) {
              proxy.setUserPasswd(config("ProxyUser"),config("proxyPassword"))
            }
            session.setProxy(proxy)
          } else {
            session.setProxy(new ProxySOCKS5(config("proxyHost")))
          }
        } else {
              if (config.contains("ProxyPort")) {
              val proxy = new ProxyHTTP(config("proxyHost"), config("proxyPort").toInt)
            if (config.contains("proxyUser")) {
              proxy.setUserPasswd(config("ProxyUser"),config("proxyPassword"))
            }
            session.setProxy(proxy)
          } else {
            session.setProxy(new ProxyHTTP(config("proxyHost")))
          }
        }
      }

      if (config.contains("sftpPassword")) {
        session.setPassword(config("sftpPassword"))
      }
      session.connect(config.getOrElse("connectionTimeout","600000").toInt)
      val channel = session.openChannel("sftp")
      channel.connect(config.getOrElse("connectionTimeout","600000").toInt)
      logger.info(s"SFTP of Session Connected(${channel.isConnected}) ..")
      sftpClient  = channel.asInstanceOf[ChannelSftp]
      val srcPath = config.getOrElse("sftpSourcePath",".")
      if (srcPath == null || ".".equals(srcPath) || srcPath.isEmpty) {
        logger.info("not provided any sourceSFTP path hence searching only in chrooted environment..")
      } else {
        sftpClient.cd(srcPath)
      }
      sftpClient.lcd(tempDir)
      val vector  = sftpClient.ls(config.getOrElse("sftpFilePattern",".*"))
      val filesToBeExtracted = vector.asInstanceOf[java.util.Vector[ChannelSftp#LsEntry]].asScala

      if (filesToBeExtracted.nonEmpty ) {
        logger.info("following Files are selected for extraction..")
        filesToBeExtracted.foreach(x => logger.info(s"${x.getFilename}"))
        for (file <- filesToBeExtracted ) {
          tempFilename = tempDir + file.getFilename
          logger.info(s"temporarily landing into ${tempDir}${file.getFilename}..")
          val fileSize = file.getAttrs.getSize
          logger.info(s"Extracting file : ${file.getFilename}, fileSize : ${fileSize/ByteSize} KB")
          if (config.contains("bucketName")){
            logger.info("bucketName Attribute is specified hence trying to load into S3, if you dont want to load to S3 please remove this property")
            sftpClient.get(file.getFilename,file.getFilename)
            val s3Utils = new AwsUtils
            s3Utils.putS3Object(config,tempFilename,config("sftpTargetPath")+"/"+file.getFilename)
            //Post load action
            if (config.getOrElse("purgeSourceFilesAfterPull","true").equalsIgnoreCase("true")) {
              logger.info(s"Purging the file ${file.getFilename} from source server..")
              sftpClient.rm(file.getFilename)
            } else if (config.getOrElse("archiveSourceFilesAfterPull","false").equalsIgnoreCase("true") && config.getOrElse("sourceFileArchivePath","").trim.nonEmpty) {
              logger.info(s"""archiving source file : ${srcPath}/${file.getFilename} to ${config("sourceFileArchivePath").trim}/${file.getFilename}""")
              sftpClient.rename(srcPath+"/"+file.getFilename,config("sourceFileArchivePath").trim+"/"+file.getFilename)
              logger.info(s"""Successfully archived file to ${config("sourceFileArchivePath").trim}/${file.getFilename}..""")
            }

          } else {
            logger.info(s"Trying to create target directory(${config("sftpTargetPath")})..")
            new File(config("sftpTargetPath")).mkdirs()
            sftpClient.get(file.getFilename,
              config("sftpTargetPath")+"/"+file.getFilename)
          }
        }
      } else {
        logger.error("No files matched in source server for the given pattern ..")
        throw new FileNotFoundException("No files matched in source server for the given pattern ..")
      }
      FileUtils.deleteDirectory(new File(tempDir))
      sftpClient.disconnect()
      session.disconnect()
      logger.info("SFTP Session disconnected..")
    } catch {
      case jshException : com.jcraft.jsch.SftpException => {
        FileUtils.deleteDirectory(new File(tempFilename))
        logger.error("No inputFiles found with given Pattern..")
        throw jshException
      }
      case e : Exception => {
        if (session != null) {
          if (sftpClient != null) {
            sftpClient.disconnect()
          }
          session.disconnect()
        }
        FileUtils.deleteDirectory(new File(tempDir))
        logger.error("Unable to read data from source Server",e)
        throw e
      }
    }
  }
}
