package co.actioniq.ivy.s3;

import com.amazonaws.auth.AWSCredentialsProviderChain;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

class RoleBasedEnvironmentVariableCredentialsProvider extends RoleBasedCredentialsProvider {
  RoleBasedEnvironmentVariableCredentialsProvider(AWSCredentialsProviderChain providerChain) {
    super(providerChain);
  }

  String RoleArnKeyName() {
    return "AWS_ROLE_ARN";
  }

  List<String> RoleArnKeyNames() {
    return Collections.singletonList(RoleArnKeyName());
  }

  protected String getRoleArn(List<String> keys) {
    return keys.stream()
        .map(k -> System.getenv(Strings.toEnvironmentVariableName(k)))
        .filter(Objects::nonNull)
        .findFirst()
        .get()
        .trim();
  }
}
