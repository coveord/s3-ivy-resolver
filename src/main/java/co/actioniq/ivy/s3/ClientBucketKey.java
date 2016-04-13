package co.actioniq.ivy.s3;

import com.amazonaws.services.s3.AmazonS3Client;

class ClientBucketKey {
  private AmazonS3Client client;
  private BucketAndKey bucketAndKey;

  ClientBucketKey(AmazonS3Client client, BucketAndKey bucketAndKey) {
    this.client = client;
    this.bucketAndKey = bucketAndKey;
  }

  AmazonS3Client getClient() {
    return client;
  }

  String getBucket() {
    return bucketAndKey.getBucket();
  }

  String getKey() {
    return bucketAndKey.getKey();
  }
}
