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

package io.github.bkosaraju.utils.aws

import java.io.{BufferedInputStream, ByteArrayInputStream, File, FileInputStream, FileOutputStream, InputStream, PipedInputStream}
import java.nio.file.Files
import java.util.UUID
import java.util.zip.GZIPOutputStream

import io.github.bkosaraju.utils.common.{Exceptions, Session}
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.regions.Regions
import com.amazonaws.services.s3.model.{CopyObjectRequest, DeleteObjectRequest, GetObjectRequest, ObjectMetadata, PutObjectRequest, S3ObjectSummary}
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder, AmazonS3URI}
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest
import io.github.bkosaraju.utils.common.{Exceptions, Session}
import org.apache.commons.codec.binary.Base64
import org.apache.commons.codec.digest.DigestUtils
import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream
import org.apache.commons.io.{FileUtils, IOUtils}
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.compress.compressors.z.ZCompressorInputStream
import org.apache.commons.compress.utils.CharsetNames
import org.apache.commons.io.output.ByteArrayOutputStream

import scala.beans.BeanProperty
import scala.collection.JavaConverters._
import resource._

class AwsUtils
  extends Session
    with Exceptions {

  @BeanProperty
  var config = Map[String, String]()

  def putS3Object(config: Map[String, String], fileName: String, keyName: String): Unit = {
    try {
      val s3Client = getS3Client(Regions.fromName(config.getOrElse("regionName", "ap-southeast-2")))
      if (config.contains("bucketName") && keyName != null && fileName != null) {
        s3Client.putObject(config("bucketName"), keyName, new File(fileName))
      } else {
        logger.error(s"Unable to load File into S3 under bucket Name(${config("bucketName")}), with given keyName(${keyName}) and fileName(${fileName})")
        throw InvalidS3MetadatException(s"Unable to load File into S3 under bucket Name(${config("bucketName")}), with given keyName(${keyName})")
      }
    }
    catch {
      case e: Exception => {
        logger.error("Unable to write data into S3..")
        throw e
      }
    }
  }

  def getS3Object(config: Map[String, String], fileName: String, keyName: String): Unit = {
    try {
      val s3Client = getS3Client(Regions.fromName(config.getOrElse("regionName", "ap-southeast-2")))
      if (config.contains("bucketName") && keyName != null && fileName != null) {

        s3Client.getObject(new GetObjectRequest(
          config("bucketName"), keyName), new File(fileName))
      } else {
        logger.error(s"Unable to get File from S3 under bucket Name(${config("bucketName")}), " +
          s"with given keyName(${keyName}) and fileName(${fileName})")
        throw InvalidS3MetadatException(s"Unable to get File from S3 under bucket Name(${config("bucketName")}), with given keyName(${keyName})")
      }
    }
    catch {
      case e: Exception => {
        logger.error("Unable to read data from S3..")
        throw e
      }
    }
  }

  def copyS3Object(config: Map[String, String]): Unit = {
    try {
      val s3Client = getS3Client(Regions.fromName(config.getOrElse("regionName", "ap-southeast-2")))
      val copyObjectReq = new CopyObjectRequest(
        config.getOrElse("sourceBucketName", config("bucketName")),
        config.getOrElse("sourceObjectKey", config("objectKey")),
        config.getOrElse("targetBucketName", config("bucketName")),
        config.getOrElse("targetObjectKey", config("objectKey"))
      )
      s3Client.copyObject(copyObjectReq)
      if (config.getOrElse("deleteSourceObject", "false").equalsIgnoreCase("true")) {
        logger.info("deleteSourceObject flag set to true hence trying to delete source object..")
        s3Client.deleteObject(config.getOrElse("sourceBucketName", config("bucketName")), config.getOrElse("sourceObjectKey", config("objectKey")))
      }
    } catch {
      case e: Exception => {
        logger.error(
          s"""Unable to copy data between using config (sourceBucket,SourceObjectKey,targetBucket,targetObjectKey)- (${config.getOrElse("sourceBucketName", config("bucketName"))},
          ${config.getOrElse("sourceObjectKey", config("objectKey"))},
          ${config.getOrElse("targetBucketName", config("bucketName"))},
          ${config.getOrElse("targetObjectKey", config("objectKey"))})""")
        throw e
      }
    }
  }

  def deleteS3Object(config: Map[String, String]): Unit = {
    try {
      if (config.contains("bucketName") && config.contains("objectKey")) {
        val s3Client = getS3Client(Regions.fromName(config.getOrElse("regionName", "ap-southeast-2")))
        val deleteObjectRequest = new DeleteObjectRequest(
          config("bucketName"),
          config("objectKey"))
        s3Client.deleteObject(deleteObjectRequest)
      } else {
        throw InvalidArgumentsPassed("mandatory arguments(bucketName,objectKey)to delete s3 object are not passed")
      }
    } catch {
      case e: Exception => {
        logger.error(s"""Unable to remove object from S3 Bucket)""")
        throw e
      }
    }
  }

  def deleteS3Object(bucketName: String, objectKey: String): Unit = {
    try {
      val s3Client = getS3Client(Regions.fromName(config.getOrElse("regionName", "ap-southeast-2")))
      val deleteObjectRequest = new DeleteObjectRequest(bucketName, objectKey)
      s3Client.deleteObject(deleteObjectRequest)
    } catch {
      case e: Exception => {
        logger.error(s"""Unable to remove object from S3 Bucket)""")
        throw e
      }
    }
  }


  def listS3Objects(bucketName: String, pathPrefix: String ): List[S3ObjectSummary] = {
    try {
      val s3Client = getS3Client(Regions.fromName(config.getOrElse("regionName", "ap-southeast-2")))
      var s3Objects = collection.mutable.ArrayBuffer[S3ObjectSummary]()

      var objectList =
        if (pathPrefix.isEmpty) {
          s3Client.listObjects(bucketName)
        } else {
          s3Client.listObjects(bucketName, pathPrefix)
        }
      s3Objects ++= objectList.getObjectSummaries.asScala
      while (objectList.isTruncated) {
        objectList = s3Client.listNextBatchOfObjects(objectList)
        s3Objects ++= objectList.getObjectSummaries.asScala
      }
      s3Objects.toList
    } catch {
      case e: Exception => {
        logger.error(s"Unable to list objects from given input bucket ${bucketName} , path prefix : ${pathPrefix}")
        throw e
      }
    }
  }


  def listS3Objects(s3Uri: String): List[S3ObjectSummary]  = {
    listS3Objects(pathToMap(s3Uri)("bucketName"),pathToMap(s3Uri).getOrElse("keyName",""))
  }

  def getS3Client(awsRegion: Regions = Regions.AP_SOUTHEAST_2): AmazonS3 = {
    try {
      val credentialsProvider = DefaultAWSCredentialsProviderChain.getInstance()

      AmazonS3ClientBuilder
        .standard().withCredentials(credentialsProvider)
        .withRegion(awsRegion)
        .build()
    } catch {
      case e: Exception => {
        logger.error("Unable to crease S3Client..")
        throw e
      }
    }
  }

  def getSSMValue(namespace: String): String = {
    try {
      val client = AWSSimpleSystemsManagementClientBuilder
        .standard()
        .withRegion(Regions.fromName(config.getOrElse("regionName", "ap-southeast-2"))).build()
      val getParameterRequest = new GetParameterRequest().withName(namespace).withWithDecryption(true)

      client.getParameter(getParameterRequest).getParameter.getValue
    } catch {
      case e: Exception => {
        logger.error(s"Unable to get the Encrypted Credentials from SSM parameter store for namespace ${namespace}", e)
        throw e
      }
    }
  }

  def getS3ObjectAsString(config: Map[String, String], keyName: String): String = {
    try {
      val s3Client = getS3Client(Regions.fromName(config.getOrElse("regionName", "ap-southeast-2")))
      if (config.contains("bucketName") && keyName != null) {
        val response = s3Client.getObject(new GetObjectRequest(config("bucketName"), keyName)).getObjectContent
        val resStream =
          if (keyName.matches(".+\\.gz$")) {
            new GzipCompressorInputStream(response)
          } else if (keyName.matches(".+\\.bz2$")) {
            new BZip2CompressorInputStream(response)
          } else if (keyName.matches(".+\\.zip$")) {
            val zpFile = new ZipArchiveInputStream(response)
            zpFile.getNextEntry
            zpFile
          } else if (keyName.matches(".+\\.[Z,z]$")) {
            new ZCompressorInputStream(response)
          } else {
            response
          }
        IOUtils.toString(resStream)
      } else {
        logger.error(s"Unable to get File from S3 under bucket Name(${config("bucketName")}), " +
          s"with given keyName(${keyName})")
        throw InvalidS3MetadatException(s"Unable to get File from S3 under bucket Name(${config("bucketName")}), with given keyName(${keyName})")
      }
    }
    catch {
      case e: Exception => {
        logger.error("Unable to read data from S3..")
        throw e
      }
    }
  }

  def getS3ObjectAsString(s3Uri: String): String = {
    getS3ObjectAsString(pathToMap(s3Uri),pathToMap(s3Uri)("keyName")
    )
  }

  def putS3ObjectFromString(config: Map[String, String], content: String): Unit = {
    val tempFileLocation =  Files.createTempDirectory("")
    try {
      var ops = collection.mutable.Map[String,String]() ++ config
      val s3Client = getS3Client(Regions.fromName(ops.getOrElse("regionName", "ap-southeast-2")))
      if (ops.contains("s3Uri")) {
        ops = ops ++ pathToMap(ops("s3Uri"))
      }
      if (ops.contains("bucketName") && ops.contains("keyName")) {
        if(ops.getOrElse("compresS3Object","true").equalsIgnoreCase("true")) {
          val localObject = new File(s"${tempFileLocation}/${UUID.randomUUID().toString}/${ops("keyName")}")
          FileUtils.writeStringToFile(localObject,content)
          for (
            contentStream <- managed(new FileInputStream(localObject));
            outputStreamHolder <- managed(new FileOutputStream(localObject+".compressed"));
            zipStream <- managed(new GZIPOutputStream(outputStreamHolder))
          ) {
            IOUtils.copy(contentStream, zipStream)
          }
          putS3Object(ops.toMap,localObject+".compressed",ops("keyName"))
        } else {
          s3Client.putObject(ops("bucketName"), ops("keyName"), content)
        }
      } else {
        logger.error(s"""Unable to write File to S3 as bucketName and KeyName are mandatory to write data""")
        throw InvalidS3MetadatException(s"""Unable to write File to S3 as bucketName and KeyName are mandatory to write data""")
      }
    } catch {
      case e : Exception => {
      logger.error("Unable to upload podlog to S3 Location..")
      throw e
      }
    } finally {
      FileUtils.deleteDirectory(tempFileLocation.toFile)
    }
  }

  def putS3ObjectFromString(config: Map[String, String]): Unit = {
    if (config.contains("content")) {
      putS3ObjectFromString(config, config("content"))
    } else {
      throw InvalidS3MetadatException(s"""Unable to get the content form given Configuration""")
      }
    }


  def pathToMap(s3Uri: String): Map[String,String] = {
    val URI = new AmazonS3URI(s3Uri)
    val region = if (URI.getRegion == null) { "ap-southeast-2" } else URI.getRegion
      Map(
        "regionName" -> region,
        "bucketName" -> URI.getBucket,
        "keyName" ->URI.getKey
      )
  }
}
