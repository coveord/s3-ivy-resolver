package co.actioniq.ivy.s3;

import org.apache.ivy.plugins.resolver.IBiblioResolver;

import java.util.List;

public class S3Resolver extends IBiblioResolver {
  public S3Resolver(String name, String root, List<String> patterns) {
    setRepository(new S3URLRepository());
    setName(name);
    setM2compatible(true);
    setRoot(root);
    setArtifactPatterns(patterns);
    setIvyPatterns(patterns);
  }

  public String getTypeName() { return "s3"; }
}
