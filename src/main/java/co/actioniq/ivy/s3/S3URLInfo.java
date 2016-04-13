package co.actioniq.ivy.s3;

import org.apache.ivy.util.url.URLHandler;

// For access to URLInfo's protected constructor
class S3URLInfo extends URLHandler.URLInfo {
  S3URLInfo(boolean available, long contentLength, long lastModified) {
    super(available, contentLength, lastModified);
  }
}
