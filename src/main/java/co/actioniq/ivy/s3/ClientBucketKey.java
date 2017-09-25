/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package co.actioniq.ivy.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import org.apache.ivy.util.Message;

import java.io.File;

class ClientBucketKey {
  private final AmazonS3Client client;
  private final BucketAndKey bucketAndKey;

  ClientBucketKey(AmazonS3Client client, BucketAndKey bucketAndKey) {
    this.client = client;
    this.bucketAndKey = bucketAndKey;
  }

  String bucket() { return bucketAndKey.bucket; }
  String key() { return bucketAndKey.key; }

  S3Object getObject(String bucketName, String key) {
    log("AIQ S3 getObject bucketName: " + bucketName + ", key: " + key);
    return client.getObject(bucketName, key);
  }

  ObjectMetadata getObject(final GetObjectRequest getObjectRequest, File destinationFile) {
    log("AIQ S3 getObject request: " + getObjectRequest + ", destinationFile: " + destinationFile);
    return client.getObject(getObjectRequest, destinationFile);
  }

  ObjectMetadata getObjectMetadata(String bucketName, String key) {
    log("AIQ S3 getObjectMetadata bucketName: " + bucketName + ", key: " + key);
    return client.getObjectMetadata(bucketName, key);
  }

  ObjectListing listObjects(ListObjectsRequest listObjectsRequest) {
    log("AIQ S3 listObjects request: " + listObjectsRequest);
    return client.listObjects(listObjectsRequest);
  }

  PutObjectResult putObject(String bucketName, String key, File file) {
    log("AIQ S3 putObject bucketName: " + bucketName + ", key: " + key + ", file: " + file);
    return client.putObject(bucketName, key, file);
  }

  TransferManager getTransferManager(String bucketName, String key) {
    log("AIQ S3 getTransferManager bucketName: " + bucketName + ", key: " + key);
    return TransferManagerBuilder.standard().withS3Client(client).build();
  }

  private void log(String message) {
    Message.info(message);
  }
}
