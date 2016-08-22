package co.actioniq.ivy.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ClearSystemProperties;
import org.junit.contrib.java.lang.system.EnvironmentVariables;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import static com.amazonaws.SDKGlobalConfiguration.*;
import static org.junit.Assert.assertEquals;

public class CredentialsTest {
  @Rule
  public final EnvironmentVariables envVars = new EnvironmentVariables();
  @Rule
  public final ClearSystemProperties clearProperties =
      new ClearSystemProperties(ACCESS_KEY_SYSTEM_PROPERTY, SECRET_KEY_SYSTEM_PROPERTY);

  private static final String data = "accessKey=abcde\n" + "secretKey=fghij";

  @Test
  public void testBasicRead() throws IOException {
    File file = File.createTempFile("foo", "bar");
    file.deleteOnExit();
    Files.write(file.toPath(), data.getBytes());

    PropertiesFileCredentialsProvider provider = new PropertiesFileCredentialsProvider(file.getPath());
    AWSCredentials credentials = provider.getCredentials();
    assertEquals("abcde", credentials.getAWSAccessKeyId());
    assertEquals("fghij", credentials.getAWSSecretKey());
  }

  @Test
  public void testChain() throws IOException {
    envVars.set(ACCESS_KEY_ENV_VAR, null);
    envVars.set(SECRET_KEY_ENV_VAR, null);

    String name = "23498sddflskdfjcsxlf1234";
    File file = new File(Constants.DotIvyDir, "." + name);
    file.deleteOnExit();
    Files.write(file.toPath(), data.getBytes());

    S3URLUtil util = new S3URLUtil(name);
    AWSCredentialsProviderChain chain = util.makeCredentialsProviderChain("foosdfsdflkj");
    AWSCredentials credentials = chain.getCredentials();
    assertEquals("abcde", credentials.getAWSAccessKeyId());
    assertEquals("fghij", credentials.getAWSSecretKey());
  }
}
