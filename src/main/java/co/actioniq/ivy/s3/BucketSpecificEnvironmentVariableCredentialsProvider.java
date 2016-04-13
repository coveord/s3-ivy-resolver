package co.actioniq.ivy.s3;

import com.amazonaws.SDKGlobalConfiguration;

import java.util.Arrays;
import java.util.Objects;

class BucketSpecificEnvironmentVariableCredentialsProvider extends BucketSpecificCredentialsProvider {
  BucketSpecificEnvironmentVariableCredentialsProvider(String bucket) {
    super(bucket);
  }

  protected String AccessKeyName() {
    return SDKGlobalConfiguration.ACCESS_KEY_ENV_VAR;
  }

  protected String SecretKeyName() {
    return SDKGlobalConfiguration.SECRET_KEY_ENV_VAR;
  }

  protected String getProp(String... names) {
    return Arrays.stream(names)
        .map(n -> System.getenv(Strings.toEnvironmentVariableName(n)))
        .filter(Objects::nonNull)
        .findFirst()
        .get()
        .trim();
  }
}
