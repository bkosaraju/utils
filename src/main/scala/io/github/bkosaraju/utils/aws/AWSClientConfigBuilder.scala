package io.github.bkosaraju.utils.aws

import com.amazonaws.ClientConfiguration

private class AWSClientConfigBuilder {

  def buildConfig(config: Map[String,String]) : ClientConfiguration = {

    val clientConfig = new ClientConfiguration()
    if (config.getOrElse("proxyHost", "").trim.nonEmpty) {
      clientConfig.setProxyHost(config("proxyHost"))
    }
    if (config.getOrElse("proxyPort", "").trim.nonEmpty) {
      clientConfig.setProxyPort(config("proxyPort").toInt)
    }
    if (config.getOrElse("proxyUser", "").trim.nonEmpty) {
      clientConfig.setProxyUsername(config("proxyUser"))
    }
    if (config.getOrElse("proxyPassword", "").trim.nonEmpty) {
      clientConfig.setProxyPassword(config("proxyPassword"))
    }
    clientConfig
  }
}

object AWSClientConfigBuilder {
  def apply(config: Map[String,String]): ClientConfiguration = {
    (new AWSClientConfigBuilder()).buildConfig(config)
  }
}



