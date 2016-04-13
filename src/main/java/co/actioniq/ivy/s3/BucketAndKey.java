package co.actioniq.ivy.s3;

class BucketAndKey {
  private String bucket;
  private String key;

  BucketAndKey(String bucket, String key) {
    this.bucket = bucket;
    this.key = key;
  }

  String getBucket() {
    return bucket;
  }

  String getKey() {
    return key;
  }
}
