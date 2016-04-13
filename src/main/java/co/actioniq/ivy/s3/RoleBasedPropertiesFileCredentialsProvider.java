package co.actioniq.ivy.s3;

import com.amazonaws.auth.AWSCredentialsProviderChain;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

class RoleBasedPropertiesFileCredentialsProvider extends RoleBasedCredentialsProvider {
  private final String fileName;

  RoleBasedPropertiesFileCredentialsProvider(AWSCredentialsProviderChain providerChain, String fileName) {
    super(providerChain);
    this.fileName = fileName;
  }

  private String RoleArnKeyName() {
    return "roleArn";
  }

  List<String> RoleArnKeyNames() {
    return Collections.singletonList(RoleArnKeyName());
  }

  String getRoleArn(List<String> keys) {
    File file = new File(Constants.DotSbtDir, fileName);

    try (InputStream is = new FileInputStream(file)) {
      Properties props = new Properties();
      props.load(is);
      // This will throw if there is no matching properties
      return RoleArnKeyNames().stream().map(props::getProperty).filter(Objects::nonNull).findFirst().get().trim();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
