package datastax.astra.migrate;

import java.nio.charset.Charset;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;

public class GoogleSecretManagerHelper {

    private static GoogleSecretManagerHelper googleSecretManagerHelper;
	public Logger logger = LoggerFactory.getLogger(this.getClass().getName());
	public static final String ASTRA_CLIENT_ID_KEY = "client_id";
	public static final String ASTRA_CLIENT_SECRET_KEY = "secret";
	public static final String ASTRA_SECRET_VERSION_ID = "latest";

	private String astraClusterUserName;
	private String astraAuthenToken;

    private GoogleSecretManagerHelper() {
		logger.info("Private constructor GoogleSecretManagerHelper invoked");
	}

    public static GoogleSecretManagerHelper getInstance() {
		if (googleSecretManagerHelper == null) {
			synchronized (GoogleSecretManagerHelper.class) {
				if (googleSecretManagerHelper == null) {
					googleSecretManagerHelper = new GoogleSecretManagerHelper();
				}
			}
		}
		return googleSecretManagerHelper;
	}

	public void populateAstraCredentials(String astraProjectId, String astraSecretManagerSecredId) throws Exception {
		try {
			JSONObject astraSecretJson = getAstraClientSecretJson(astraProjectId, astraSecretManagerSecredId);
			astraClusterUserName = astraSecretJson.getString(ASTRA_CLIENT_ID_KEY);
			astraAuthenToken = astraSecretJson.getString(ASTRA_CLIENT_SECRET_KEY);
		} catch (JSONException e) {
			throw new Exception("Error Occured while extracting client Id/secret from Json:" + e);
		} catch (Exception e) {
			throw e;
		}
	}

	public String getAstraClusterUserName() {
		return astraClusterUserName;
	}

	public String getAstraAuthenToken() {
		return astraAuthenToken;
	}

	private JSONObject getAstraClientSecretJson(String projectId, String secretId) throws Exception {
		JSONObject astraSecretJson = new JSONObject();
		if (secretId == null || projectId == null) {
			logger.error(
					"Required Google Secret Manager properties are missing, please check.");
		} else {
			astraSecretJson = getClientSecretJson(projectId, secretId);
		}
		return astraSecretJson;
	}

	private JSONObject getClientSecretJson(String projectId, String secretId) throws Exception {
		JSONObject clientSecJsonObj = new JSONObject();
		ByteString payloadData = accessSecretVersion(projectId, secretId, ASTRA_SECRET_VERSION_ID);
		if (ByteString.EMPTY != payloadData)
			clientSecJsonObj = new JSONObject(payloadData.toString(Charset.defaultCharset()));
		return clientSecJsonObj;
	}

	private ByteString accessSecretVersion(String projectId, String secretId, String version) throws Exception {
		ByteString payloadData = ByteString.EMPTY;
		try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
			SecretVersionName secretVersionName = SecretVersionName.of(projectId, secretId, version);
			// Access the secret version.
			AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);
			payloadData = response.getPayload().getData();
			long payloadChecksum = response.getPayload().getDataCrc32C() & 0xFFFFFFFFL;
			if (!validChecksum(payloadData, payloadChecksum))
				throw new Exception(
						"The checksum received is invalid for Google Secret Paylod. Corrupted Data Detected!!");
		}
		return payloadData;
	}

	private boolean validChecksum(ByteString data, long crc32Checksum) {
		long checksum = Hashing.crc32c().hashBytes(data.toByteArray()).asInt() & 0xFFFFFFFFL; // to convert it into long appended 0xFFFFFFFFL
		return checksum == crc32Checksum;
	}

}
