package co.actioniq.ivy.s3;

import com.amazonaws.services.s3.AmazonS3Client;

class ClientBucketKey {
  final AmazonS3Client client;
  final BucketAndKey bucketAndKey;

  ClientBucketKey(AmazonS3Client client, BucketAndKey bucketAndKey) {
    this.client = client;
    this.bucketAndKey = bucketAndKey;
  }

  String bucket() { return bucketAndKey.bucket; }
  String key() { return bucketAndKey.key; }
}
