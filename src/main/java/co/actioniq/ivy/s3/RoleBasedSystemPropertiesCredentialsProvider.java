package co.actioniq.ivy.s3;

import com.amazonaws.auth.AWSCredentialsProviderChain;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

class RoleBasedSystemPropertiesCredentialsProvider extends RoleBasedCredentialsProvider {
  RoleBasedSystemPropertiesCredentialsProvider(AWSCredentialsProviderChain providerChain) {
    super(providerChain);
  }

  String RoleArnKeyName() {
    return "aws.roleArn";
  }

  List<String> RoleArnKeyNames() {
    return Collections.singletonList(RoleArnKeyName());
  }

  String getRoleArn(List<String> keys) {
    return keys.stream().map(System::getProperty).filter(Objects::nonNull).findFirst().get().trim();
  }
}
