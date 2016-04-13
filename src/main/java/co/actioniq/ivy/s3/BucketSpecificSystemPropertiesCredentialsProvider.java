package co.actioniq.ivy.s3;

import com.amazonaws.SDKGlobalConfiguration;

import java.util.Arrays;
import java.util.Objects;

class BucketSpecificSystemPropertiesCredentialsProvider extends BucketSpecificCredentialsProvider {
  BucketSpecificSystemPropertiesCredentialsProvider(String bucket) {
    super(bucket);
  }

  protected String AccessKeyName() {
    return SDKGlobalConfiguration.ACCESS_KEY_SYSTEM_PROPERTY;
  }

  protected String SecretKeyName() {
    return SDKGlobalConfiguration.SECRET_KEY_SYSTEM_PROPERTY;
  }

  protected String getProp(String... names) {
    return Arrays.stream(names).map(System::getProperty).filter(Objects::nonNull).findFirst().get().trim();
  }
}
