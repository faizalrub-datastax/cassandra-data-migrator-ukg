package datastax.astra.migrate

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.SparkConf

class AbstractJob extends BaseJob {

  val ASTRA_SECRET_VERSION_ID: String = "latest"

  abstractLogger.info("PARAM -- Min Partition: " + minPartition)
  abstractLogger.info("PARAM -- Max Partition: " + maxPartition)
  abstractLogger.info("PARAM -- Split Size: " + splitSize)
  abstractLogger.info("PARAM -- Coverage Percent: " + coveragePercent)  

  var sourceConnection = getConnection(true, sourceScbPath, sourceHost, sourceUsername, sourcePassword,
    sourceTrustStorePath, sourceTrustStorePassword, sourceTrustStoreType, sourceKeyStorePath, sourceKeyStorePassword, 
    sourceEnabledAlgorithms, gsmProjectId, secretName);

  var destinationConnection = getConnection(false, destinationScbPath, destinationHost, destinationUsername, destinationPassword,
    destinationTrustStorePath, destinationTrustStorePassword, destinationTrustStoreType, destinationKeyStorePath, 
    destinationKeyStorePassword, destinationEnabledAlgorithms, gsmProjectId, secretName);

  private def getConnection(isSource: Boolean, scbPath: String, host: String, username: String, password: String,
                            trustStorePath: String, trustStorePassword: String, trustStoreType: String,
                            keyStorePath: String, keyStorePassword: String, enabledAlgorithms: String, gsmProjectId: String,
                            secretName: String): CassandraConnector = {
    
    var connType: String = "Source"
    if (!isSource) {
      connType = "Destination"
    }

    var config: SparkConf = sContext.getConf
    if (scbPath.nonEmpty) {

      abstractLogger.info(connType + ": Connecting to Astra using SCB: " + scbPath);
      abstractLogger.info("GSM Project Id Value : "+gsmProjectId)
      abstractLogger.info("Secret Name : "+secretName)
      if(gsmProjectId.nonEmpty && secretName.nonEmpty){

      	abstractLogger.info(connType + ": Connecting to Astra using secure manager : ");
	    GoogleSecretManagerHelper.getInstance().populateAstraCredentials(gsmProjectId,secretName)
        val id: String = GoogleSecretManagerHelper.getInstance().getAstraClusterUserName()
        val secret: String = GoogleSecretManagerHelper.getInstance().getAstraAuthenToken()

        if(id.nonEmpty && secret.nonEmpty){
          abstractLogger.info("Connecting using GSM");
          return CassandraConnector(config
          .set("spark.cassandra.auth.username", id)
          .set("spark.cassandra.auth.password", secret)
          .set("spark.cassandra.input.consistency.level", consistencyLevel)
          .set("spark.cassandra.connection.config.cloud.path", scbPath))
        }else{
          abstractLogger.info("Connecting using USERNAME and PASSWORD");
          return CassandraConnector(config
          .set("spark.cassandra.auth.username", username)
          .set("spark.cassandra.auth.password", password)
          .set("spark.cassandra.input.consistency.level", consistencyLevel)
          .set("spark.cassandra.connection.config.cloud.path", scbPath))
        }
      }
      return CassandraConnector(config
        .set("spark.cassandra.auth.username", username)
        .set("spark.cassandra.auth.password", password)
        .set("spark.cassandra.input.consistency.level", consistencyLevel)
        .set("spark.cassandra.connection.config.cloud.path", scbPath))
    } else if (trustStorePath.nonEmpty) {
      abstractLogger.info(connType + ": Connecting to Cassandra (or DSE) with SSL host: " + host);

      // Use defaults when not provided
      var enabledAlgorithmsVar = enabledAlgorithms
      if (enabledAlgorithms == null || enabledAlgorithms.trim.isEmpty) {
        enabledAlgorithmsVar = "TLS_RSA_WITH_AES_128_CBC_SHA, TLS_RSA_WITH_AES_256_CBC_SHA"
      }

      return CassandraConnector(config
        .set("spark.cassandra.auth.username", username)
        .set("spark.cassandra.auth.password", password)
        .set("spark.cassandra.input.consistency.level", consistencyLevel)
        .set("spark.cassandra.connection.host", host)
        .set("spark.cassandra.connection.ssl.enabled", "true")
        .set("spark.cassandra.connection.ssl.enabledAlgorithms", enabledAlgorithmsVar)
        .set("spark.cassandra.connection.ssl.trustStore.password", trustStorePassword)
        .set("spark.cassandra.connection.ssl.trustStore.path", trustStorePath)
        .set("spark.cassandra.connection.ssl.keyStore.password", keyStorePassword)
        .set("spark.cassandra.connection.ssl.keyStore.path", keyStorePath)
        .set("spark.cassandra.connection.ssl.trustStore.type", trustStoreType)
        .set("spark.cassandra.connection.ssl.clientAuth.enabled", "true")
      )
    } else {
      abstractLogger.info(connType + ": Connecting to Cassandra (or DSE) host: " + host);

      return CassandraConnector(config.set("spark.cassandra.auth.username", username)
        .set("spark.cassandra.auth.password", password)
        .set("spark.cassandra.input.consistency.level", consistencyLevel)
        .set("spark.cassandra.connection.host", host))
    }

  }

}
