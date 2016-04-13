package co.actioniq.ivy.s3;

import com.amazonaws.auth.AWSCredentialsProviderChain;

import java.util.Arrays;
import java.util.List;

class BucketSpecificRoleBasedSystemPropertiesCredentialsProvider extends RoleBasedSystemPropertiesCredentialsProvider {
  private final String bucket;

  BucketSpecificRoleBasedSystemPropertiesCredentialsProvider(AWSCredentialsProviderChain providerChain, String bucket) {
    super(providerChain);
    this.bucket = bucket;
  }

  List<String> RoleArnKeyNames() {
    return Arrays.asList(RoleArnKeyName() + "." + bucket, bucket + "." + RoleArnKeyName());
  }
}
