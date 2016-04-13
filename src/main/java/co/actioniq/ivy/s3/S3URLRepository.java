package co.actioniq.ivy.s3;

import org.apache.ivy.plugins.repository.url.URLRepository;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;

class S3URLRepository extends URLRepository {
  private final S3URLHandler s3 = new S3URLHandler();

  public List list(String parent) throws IOException {
    if (parent.startsWith("s3")) {
      return s3.list(new URL(parent)).stream().map(URL::toExternalForm).collect(Collectors.toList());
    } else {
      return super.list(parent);
    }
  }
}
