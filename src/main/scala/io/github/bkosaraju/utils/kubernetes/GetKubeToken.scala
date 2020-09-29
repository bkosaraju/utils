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

import java.util.{Base64, Calendar, Date, UUID}

import com.amazonaws.auth.{AWSCredentialsProvider, DefaultAWSCredentialsProviderChain, STSAssumeRoleSessionCredentialsProvider, SdkClock, SignerFactory, SignerParams}
import com.amazonaws.auth.STSAssumeRoleSessionCredentialsProvider.Builder
import com.amazonaws.regions.Regions
import com.amazonaws.services.eks.{AmazonEKS, AmazonEKSClientBuilder}
import com.amazonaws.services.eks.model.{Cluster, DescribeClusterRequest}
import com.amazonaws.services.securitytoken.{AWSSecurityTokenService, AWSSecurityTokenServiceClient, AWSSecurityTokenServiceClientBuilder}
import com.amazonaws.{ClientConfiguration, DefaultRequest}
import com.amazonaws.services.securitytoken.model.{AssumeRoleRequest, GetCallerIdentityRequest}
import java.net.URI

import com.amazonaws.http.HttpMethodName
import com.amazonaws.internal.auth.DefaultSignerProvider
import com.amazonaws.auth.presign.{PresignerFacade, PresignerParams}


class GetKubeToken(kubeSysConfiguration: Map[String,String]) extends Config {

    val securityTokenService : AWSSecurityTokenService = AWSSecurityTokenServiceClientBuilder.standard()
      .withRegion((kubeSysConfiguration.getOrElse("region",DEFAULT_REGION.getName)))
      .withCredentials(credentialsProvider)
      .build();

    val stsAssumeRoleScesCreds = new STSAssumeRoleSessionCredentialsProvider.Builder(
      kubeSysConfiguration.getOrElse("applicationIamRole",APPLICATION_IAM_ROLE),
      kubeSysConfiguration.getOrElse("kubeUser","User")+"_"+UUID.randomUUID().toString
    ).withStsClient(securityTokenService).build()

  val awsSecurityTokenServiceClient : AWSSecurityTokenServiceClient =
    AWSSecurityTokenServiceClientBuilder
      .standard()
      .withRegion(kubeSysConfiguration.getOrElse("region",DEFAULT_REGION.getName))
      .withCredentials(stsAssumeRoleScesCreds)
      .build()
      .asInstanceOf[AWSSecurityTokenServiceClient]



  def getToken : String = {
    getKubeToken(
      EKSCluster(kubeSysConfiguration).getName, getExpiryDate(kubeSysConfiguration.getOrElse("kubeTokenExpiryDuration",KUBE_TOKEN_EXPIRY_DURATION).toString.toInt)
      ,REQUEST_SERVICE_NAME,
      kubeSysConfiguration.getOrElse("region", DEFAULT_REGION.getName),
      awsSecurityTokenServiceClient,
      stsAssumeRoleScesCreds,
      kubeSysConfiguration.getOrElse("stsHost", "sts.ap-southeast-2.amazonaws.com")
    )
  }

  def getExpiryDate( duration: Long = KUBE_TOKEN_EXPIRY_DURATION): Date = {
  val currentTS  = Calendar.getInstance().getTimeInMillis
    new Date(currentTS + (duration * 1000))
  }

  def getKubeToken (eksClusterName: String,
                      expirationDate: Date,
                      serviceName: String,
                      region: String,
                      awsSecurityTokenServiceClient: AWSSecurityTokenServiceClient,
                      credentialsProvider: AWSCredentialsProvider,
                      stsHost: String = "sts.ap-southeast-2.amazonaws.com"
                    ) : String = {

    val  callerIdentityRequestDefaultRequest =  new DefaultRequest[GetCallerIdentityRequest](
        new GetCallerIdentityRequest(),
        serviceName
      )

    val stsURI = new URI("https", stsHost, null, null)
    callerIdentityRequestDefaultRequest.setResourcePath("/")
    callerIdentityRequestDefaultRequest.setEndpoint(stsURI)
    callerIdentityRequestDefaultRequest.setHttpMethod(HttpMethodName.GET)
    callerIdentityRequestDefaultRequest.addParameter("Action", "GetCallerIdentity")
    callerIdentityRequestDefaultRequest.addParameter("Version", "2011-06-15")
    callerIdentityRequestDefaultRequest.addHeader("x-k8s-aws-id", eksClusterName)

    val signer = SignerFactory
      .createSigner(
        SignerFactory.VERSION_FOUR_SIGNER,
        new SignerParams(
          serviceName,
          kubeSysConfiguration.getOrElse("region",DEFAULT_REGION.getName)
        )
      )

    val signerProvider = new DefaultSignerProvider(awsSecurityTokenServiceClient, signer)
    val presignerParams = new PresignerParams(
      stsURI,
      credentialsProvider,
      signerProvider,
      SdkClock.STANDARD
    )
    val  presignerFacade = new PresignerFacade(presignerParams)
    val tokenUrl = presignerFacade.presign(callerIdentityRequestDefaultRequest, expirationDate)
    "k8s-aws-v1." + Base64.getUrlEncoder.withoutPadding().encodeToString(tokenUrl.toString.getBytes())
  }
}

object GetKubeToken {
  def apply(config: Map[String,String]): String = { new GetKubeToken(config).getToken }
}


