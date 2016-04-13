package co.actioniq.ivy.s3;

import com.amazonaws.auth.AWSCredentialsProviderChain;

import java.util.Arrays;
import java.util.List;

class BucketSpecificRoleBasedEnvironmentVariableCredentialsProvider extends RoleBasedEnvironmentVariableCredentialsProvider {
  private final String bucket;

  BucketSpecificRoleBasedEnvironmentVariableCredentialsProvider(AWSCredentialsProviderChain providerChain, String bucket) {
    super(providerChain);
    this.bucket = bucket;
  }

  List<String> RoleArnKeyNames() {
    return Arrays.asList(RoleArnKeyName() + "." + bucket, bucket + "." + RoleArnKeyName());
  }
}
