package co.actioniq.ivy.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.Throwables;
import org.apache.ivy.util.CopyProgressEvent;
import org.apache.ivy.util.CopyProgressListener;
import org.apache.ivy.util.Message;
import org.apache.ivy.util.url.URLHandler;

import java.io.File;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Optional;

class S3URLHandler implements URLHandler {
  // This is for matching region names in URLs or host names
  private static Regex regionMatcher = Regions.values().map((x) -> x.getName()).sortBy

  {
    -1 * _.length
  }

  .

  mkString("|")

  .r

  public boolean isReachable(URL url) {
    return getURLInfo(url).isReachable();
  }

  public boolean isReachable(URL url, int timeout) {
    return getURLInfo(url, timeout).isReachable();
  }

  public long getContentLength(URL url) {
    return getURLInfo(url).getContentLength();
  }

  public long getContentLength(URL url, int timeout) {
    return getURLInfo(url, timeout).getContentLength();
  }

  public long getLastModified(URL url) {
    return getURLInfo(url).getLastModified();
  }

  public long getLastModified(URL url, int timeout) {
    return getURLInfo(url, timeout).getLastModified();
  }

  public URLInfo getURLInfo(URL url) {
    return getURLInfo(url, 0);
  }

  private void debug(String msg) {
    Message.debug("S3URLHandler." + msg);
  }

  private PropertiesFileCredentialsProvider makePropertiesFileCredentialsProvider(String fileName) {
    File dir = new File(System.getProperty("user.home"), ".sbt");
    File file = new File(dir, fileName);
    return new PropertiesFileCredentialsProvider(file.toString());
  }

  private AWSCredentialsProviderChain makeCredentialsProviderChain(String bucket) {
    return new AWSCredentialsProviderChain(
        new BucketSpecificEnvironmentVariableCredentialsProvider(bucket),
        new BucketSpecificSystemPropertiesCredentialsProvider(bucket),
        makePropertiesFileCredentialsProvider(".s3credentials_" + bucket),
        makePropertiesFileCredentialsProvider("." + bucket + "_s3credentials"),
        new EnvironmentVariableCredentialsProvider(),
        new SystemPropertiesCredentialsProvider(),
        makePropertiesFileCredentialsProvider(".s3credentials"),
        new InstanceProfileCredentialsProvider()
    );
  }

  AWSCredentials getCredentials(String bucket) {
    try {
      return makeCredentialsProviderChain(bucket).getCredentials();
    } catch (AmazonClientException e) {
      Message.error("Unable to find AWS Credentials.");
      throw e;
    }
  }


  ClientBucketKey getClientBucketAndKey(URL url) {
    BucketAndKey bk = getBucketAndKey(url);
    AmazonS3Client client = new AmazonS3Client(getCredentials(bk.getBucket()));
    Optional<Region> region = getRegion(url, bk.getBucket(), client);
    region.ifPresent((r) -> client.setRegion(r));
    return new ClientBucketKey(client, bk);
  }

  public URLInfo getURLInfo(URL url, int timeout) {
    try {
      debug("getURLInfo(" + url + ", " + timeout + ")");

      ClientBucketKey cbk = getClientBucketAndKey(url);

      ObjectMetadata meta = cbk.getClient().getObjectMetadata(cbk.getBucket(), cbk.getKey());

      long contentLength = meta.getContentLength();
      long lastModified = meta.getLastModified().getTime();

      return new S3URLInfo(true, contentLength, lastModified);
    } catch (AmazonS3Exception e) {
      if (e.getStatusCode() == 404) {
        return UNAVAILABLE;
      }
    }
  }

  public InputStream openStream(URL url) {
    debug("openStream(" + url + ")");

    ClientBucketKey cbk = getClientBucketAndKey(url);
    S3Object obj = cbk.getClient().getObject(cbk.getBucket(), cbk.getKey());
    return obj.getObjectContent();
  }

  /**
   * A directory listing for keys/directories under this prefix
   */
  public List<URL> list(URL url) {
    debug("list(" + url + ")");

      /* key is the prefix in this case */
    ClientBucketKey cbk = getClientBucketAndKey(url);

    // We want the prefix to have a trailing slash
    String prefix = Strings.stripSuffix(cbk.getKey(), "/") + "/";

    ListObjectsRequest request = new ListObjectsRequest().withBucketName(cbk.getBucket()).withPrefix(prefix).withDelimiter("/");

    ObjectListing listing = cbk.getClient().listObjects(request);

    assert (!listing.isTruncated(),"Truncated ObjectListing!  Making additional calls currently isn't implemented!");

    List<String> keys = listing.getCommonPrefixes().addAll(listing.getObjectSummaries().map((s) -> s.getKey()));

    List<URL> res = keys.map((k) ->
        new URL(Strings.stripSuffix(url.toString(), "/") + "/" + Strings.stripPrefix(k, prefix)));

    debug("list(" + url + ") => \n  " + res.mkString("\n  "));

    return res;
  }

  public void download(URL src, File dest, CopyProgressListener l) {
    debug("download(" + src + ", " + dest + ")");

    ClientBucketKey cbk = getClientBucketAndKey(src);
    cbk

    CopyProgressEvent event = new CopyProgressEvent();
    if (null != l) {
      l.start(event);
    }

    ObjectMetadata meta = cbk.getClient().getObject(new GetObjectRequest(cbk.getBucket(), cbk.getKey()), dest);
    dest.setLastModified(meta.getLastModified().getTime());

    if (null != l) l.end(event); //l.progress(evt.update(EMPTY_BUFFER, 0, meta.getContentLength))
  }

  public void upload(File src, URL dest, CopyProgressListener l) {
    debug("upload(" + src + ", " + dest + ")");

    CopyProgressEvent event = new CopyProgressEvent();
    if (null != l) {
      l.start(event);
    }

    ClientBucketKey cbk = getClientBucketAndKey(dest);
    PutObjectResult res = cbk.getClient().putObject(cbk.getBucket(), cbk.getKey(), src);

    if (null != l) l.end(event);
  }

  // I don't think we care what this is set to
  public void setRequestMethod(int requestMethod) {
    debug("setRequestMethod(" + requestMethod + ")");
  }

  // Try to get the region of the S3 URL so we can set it on the S3Client
  Optional<Region> getRegion(URL url, String bucket, AmazonS3Client client) {
    Optional<String> region = getRegionNameFromURL(url)
        .orElse(() -> getRegionNameFromDNS(bucket))
        .orElse(() -> getRegionNameFromService(bucket, client));
    return region.map((r) -> RegionUtils.getRegion(r));
  }

  Optional<String> getRegionNameFromURL(URL url) {
    // We'll try the AmazonS3URI parsing first then fallback to our RegionMatcher
    return getAmazonS3URI(url).map((x) -> x.getRegion()).orElse((x) -> RegionMatcher.findFirstIn(url.toString()));
  }

  Optional<String> getRegionNameFromDNSString(String bucket) {
    try {
      // This gives us something like s3-us-west-2-w.amazonaws.com which must have changed
      // at some point because the region from that hostname is no longer parsed by AmazonS3URI
      String canonicalHostName = InetAddress.getByName(bucket + ".s3.amazonaws.com").getCanonicalHostName();

      // So we use our regex based RegionMatcher to try and extract the region since AmazonS3URI doesn't work
      return RegionMatcher.findFirstIn(canonicalHostName);
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  // TODO: cache the result of this so we aren't always making the call
  Optional<String> getRegionNameFromService(String bucket, AmazonS3Client client) {
    try {
      // This might fail if the current credentials don't have access to the getBucketLocation call
      return Optional.of(client.getBucketLocation(bucket));
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  BucketAndKey getBucketAndKey(URL url) {
    // The AmazonS3URI constructor should work for standard S3 urls.  But if a custom domain is being used
    // (e.g. snapshots.maven.frugalmechanic.com) then we treat the hostname as the bucket and the path as the key
    return getAmazonS3URI(url)
        .map((amzn) -> new BucketAndKey(amzn.getBucket(), amzn.getKey()))
        .orElseGet(() -> new BucketAndKey(url.getHost(), Strings.stripPrefix(url.getPath(), "/")));
  }

  Optional<AmazonS3URI> getAmazonS3URI(String uri) {
    return getAmazonS3URI(URI.create(uri));
  }

  Optional<AmazonS3URI> getAmazonS3URI(URL url) {
    try {
      return getAmazonS3URI(url.toURI());
    } catch (URISyntaxException e) {
      return Optional.empty();
    }
  }

  Optional<AmazonS3URI> getAmazonS3URI(URI uri) {
    URI httpsURI;
    try {
      // If there is no scheme (e.g. new URI("s3-us-west-2.amazonaws.com/<bucket>"))
      // then we need to re-create the URI to add one and to also make sure the host is set
      if (uri.getScheme() == null) {
        httpsURI = new URI("https://" + uri);
      } else {
        // AmazonS3URI can't parse the region from s3:// URLs so we rewrite the scheme to https://
        httpsURI = new URI("https", uri.getUserInfo(), uri.getHost(), uri.getPort(),
            uri.getPath(), uri.getQuery(), uri.getFragment());
      }
      return Optional.of(new AmazonS3URI(httpsURI));
    } catch (URISyntaxException e) {
      return Optional.empty();
    }
  }
}