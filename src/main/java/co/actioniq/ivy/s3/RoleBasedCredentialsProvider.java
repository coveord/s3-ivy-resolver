package co.actioniq.ivy.s3;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClient;
import com.amazonaws.services.securitytoken.model.AssumeRoleRequest;
import com.amazonaws.services.securitytoken.model.AssumeRoleResult;

import java.util.List;

abstract class RoleBasedCredentialsProvider implements AWSCredentialsProvider {
  private final AWSCredentialsProviderChain providerChain;

  RoleBasedCredentialsProvider(AWSCredentialsProviderChain providerChain) {
    this.providerChain = providerChain;
  }

  abstract List<String> RoleArnKeyNames();

  // This should throw an exception if the value is missing
  abstract String getRoleArn(List<String> keys);

  public AWSCredentials getCredentials() {
    AWSSecurityTokenServiceClient securityTokenService = new AWSSecurityTokenServiceClient(providerChain);

    AssumeRoleRequest roleRequest = new AssumeRoleRequest()
        .withRoleArn(getRoleArn(RoleArnKeyNames()))
        .withRoleSessionName(String.valueOf(System.currentTimeMillis()));

    AssumeRoleResult result = securityTokenService.assumeRole(roleRequest);

    return new BasicSessionCredentials(result.getCredentials().getAccessKeyId(),
        result.getCredentials().getSecretAccessKey(),
        result.getCredentials().getSessionToken());
  }

  public void refresh() {
  }
}
