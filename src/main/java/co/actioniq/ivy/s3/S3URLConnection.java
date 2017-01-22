package co.actioniq.ivy.s3;

import com.amazonaws.services.s3.model.S3Object;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.net.URLConnection;

class S3URLConnection extends URLConnection {
  private S3URLUtil s3URLUtil = new S3URLUtil();

  S3URLConnection(URL url) {
    super(url);
  }

  @Override
  public InputStream getInputStream() throws IOException {
    ClientBucketKey clientBucketKey = s3URLUtil.getClientBucketAndKey(url);
    S3Object object = clientBucketKey.getObject(clientBucketKey.bucket(), clientBucketKey.key());
    return object.getObjectContent();
  }

  @Override
  public void connect() throws IOException {
    // do nothing
  }
}
