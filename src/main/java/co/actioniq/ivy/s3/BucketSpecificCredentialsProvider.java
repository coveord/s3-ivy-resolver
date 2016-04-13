package co.actioniq.ivy.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;

abstract class BucketSpecificCredentialsProvider implements AWSCredentialsProvider {
  private final String bucket;

  BucketSpecificCredentialsProvider(String bucket) {
    this.bucket = bucket;
  }

  public AWSCredentials getCredentials() {
    String accessKey = getProp(AccessKeyName() + "." + bucket, bucket + "." + AccessKeyName());
    String secretKey = getProp(SecretKeyName() + "." + bucket, bucket + "." + SecretKeyName());
    return new BasicAWSCredentials(accessKey, secretKey);
  }

  public void refresh() {}

  protected abstract String AccessKeyName();

  protected abstract String SecretKeyName();

  // This should throw an exception if the value is missing
  protected abstract String getProp(String... names);
}
