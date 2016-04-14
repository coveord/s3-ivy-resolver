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

import com.amazonaws.AmazonClientException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.PropertiesFileCredentialsProvider;
import com.amazonaws.auth.SystemPropertiesCredentialsProvider;
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
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.ivy.util.CopyProgressEvent;
import org.apache.ivy.util.CopyProgressListener;
import org.apache.ivy.util.Message;
import org.apache.ivy.util.url.URLHandler;
import org.apache.ivy.util.url.URLHandlerDispatcher;
import org.apache.ivy.util.url.URLHandlerRegistry;

import java.io.File;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class S3URLHandler implements URLHandler {
  // One time setup to register our handler for S3:// urls in Ivy
  private static final boolean _init = initHandlers();

  // This is for matching region names in URLs or host names
  private static final Pattern RegionMatcher = makeRegionMatcher();

  private static Pattern makeRegionMatcher() {
    Comparator<String> ReverseLengthComparator = Comparator.comparingInt(String::length).reversed();
    String pattern = Arrays.stream(Regions.values())
        .map(Regions::getName)
        .sorted(ReverseLengthComparator)
        .collect(Collectors.joining("|", "(", ")"));
    return Pattern.compile(pattern);
  }

  private static boolean initHandlers() {
    initDispatcher();
    initStreamHandler();
    return true;
  }

  private static void initDispatcher() {
    URLHandler defaultHandler = URLHandlerRegistry.getDefault();
    URLHandlerDispatcher dispatcher;
    if (defaultHandler instanceof URLHandlerDispatcher) {
      info("Using the existing Ivy URLHandlerDispatcher to handle s3:// URLs");
      dispatcher = (URLHandlerDispatcher)defaultHandler;
    } else {
      info("Creating a new Ivy URLHandlerDispatcher to handle s3:// URLs");
      dispatcher = new URLHandlerDispatcher();
      dispatcher.setDefault(defaultHandler);
      URLHandlerRegistry.setDefault(dispatcher);
    }
    dispatcher.setDownloader("s3", new S3URLHandler());
  }

  private static void initStreamHandler() {
    // We need s3:// URLs to work without throwing a java.net.MalformedURLException
    // which means installing a dummy URLStreamHandler.  We only install the handler
    // if it's not already installed (since a second call to URL.setURLStreamHandlerFactory
    // will fail).
    try {
      new URL("s3://example.com");
      info("The s3:// URLStreamHandler is already installed");
    } catch (MalformedURLException e) {
      // This means we haven't installed the handler, so install it
        info("Installing the s3:// URLStreamHandler via java.net.URL.setURLStreamHandlerFactory");
        URL.setURLStreamHandlerFactory(new S3URLStreamHandlerFactory());
    }
  }

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

  private static void info(String msg) {
    Message.info(msg);
  }

  private static void debug(String msg) {
    Message.debug("S3URLHandler." + msg);
  }

  private PropertiesFileCredentialsProvider makePropertiesFileCredentialsProvider(String fileName) {
    File file = new File(Constants.DotIvyDir, fileName);
    return new PropertiesFileCredentialsProvider(file.toString());
  }

  private AWSCredentialsProviderChain makeCredentialsProviderChain(String bucket) {
    AWSCredentialsProvider[] basicProviders = {
        new BucketSpecificEnvironmentVariableCredentialsProvider(bucket),
        new BucketSpecificSystemPropertiesCredentialsProvider(bucket),
        makePropertiesFileCredentialsProvider(".s3credentials_" + bucket),
        makePropertiesFileCredentialsProvider("." + bucket + "_s3credentials"),
        new EnvironmentVariableCredentialsProvider(),
        new SystemPropertiesCredentialsProvider(),
        makePropertiesFileCredentialsProvider(".s3credentials"),
        new InstanceProfileCredentialsProvider()
    };

    AWSCredentialsProviderChain basicProviderChain = new AWSCredentialsProviderChain(basicProviders);

    AWSCredentialsProvider[] roleProviders = {
        new BucketSpecificRoleBasedEnvironmentVariableCredentialsProvider(basicProviderChain, bucket),
        new BucketSpecificRoleBasedSystemPropertiesCredentialsProvider(basicProviderChain, bucket),
        new RoleBasedPropertiesFileCredentialsProvider(basicProviderChain, ".s3credentials_" + bucket),
        new RoleBasedPropertiesFileCredentialsProvider(basicProviderChain, "." + bucket + "_s3credentials"),
        new RoleBasedEnvironmentVariableCredentialsProvider(basicProviderChain),
        new RoleBasedSystemPropertiesCredentialsProvider(basicProviderChain),
        new RoleBasedPropertiesFileCredentialsProvider(basicProviderChain, ".s3credentials")
    };

    List<AWSCredentialsProvider> providerList = new ArrayList<>();
    providerList.addAll(Arrays.asList(roleProviders));
    providerList.addAll(Arrays.asList(basicProviders));
    AWSCredentialsProvider[] providerArray = providerList.toArray(new AWSCredentialsProvider[providerList.size()]);

    return new AWSCredentialsProviderChain(providerArray);
  }

  private Map<String,AWSCredentials> credentialsCache = new ConcurrentHashMap<>();

  private AWSCredentials getCredentials(String bucket) {
    AWSCredentials credentials = credentialsCache.computeIfAbsent(bucket, this::computeCredentials);
    Message.info("S3URLHandler - Using AWS Access Key Id: "+credentials.getAWSAccessKeyId()+" for bucket: "+bucket);
    return credentials;
  }

  private AWSCredentials computeCredentials(String bucket) {
    try {
      return makeCredentialsProviderChain(bucket).getCredentials();
    } catch (AmazonClientException e) {
      Message.error("Unable to find AWS Credentials.");
      throw e;
    }
  }

  private ClientConfiguration getProxyConfiguration() {
    ClientConfiguration configuration = new ClientConfiguration();
    Optional<String> host = Optional.ofNullable(System.getProperty("https.proxyHost"));
    Optional<Integer> port = Optional.ofNullable(System.getProperty("https.proxyPort")).map(Integer::parseInt);
    if (host.isPresent() && port.isPresent()) {
      configuration.setProxyHost(host.get());
      configuration.setProxyPort(port.get());
    }
    return configuration;
  }

  private ClientBucketKey getClientBucketAndKey(URL url) {
    BucketAndKey bk = getBucketAndKey(url);
    AmazonS3Client client = new AmazonS3Client(getCredentials(bk.bucket), getProxyConfiguration());
    Optional<Region> region = getRegion(url, bk.bucket, client);
    region.ifPresent(client::setRegion);
    return new ClientBucketKey(client, bk);
  }


  public URLInfo getURLInfo(URL url, int timeout) {
    try {
      debug("getURLInfo(" + url + ", " + timeout + ")");

      ClientBucketKey cbk = getClientBucketAndKey(url);

      ObjectMetadata meta = cbk.client.getObjectMetadata(cbk.bucket(), cbk.key());

      long contentLength = meta.getContentLength();
      long lastModified = meta.getLastModified().getTime();

      return new S3URLInfo(true, contentLength, lastModified);
    } catch (AmazonS3Exception e) {
      if (e.getStatusCode() == 404) {
        return UNAVAILABLE;
      }
      throw e;
    }
  }

  public InputStream openStream(URL url) {
    debug("openStream(" + url + ")");

    ClientBucketKey cbk = getClientBucketAndKey(url);
    S3Object obj = cbk.client.getObject(cbk.bucket(), cbk.key());
    return obj.getObjectContent();
  }

  /**
   * A directory listing for keys/directories under this prefix
   */
  List<URL> list(URL url) throws MalformedURLException {
    debug("list(" + url + ")");

      /* key is the prefix in this case */
    ClientBucketKey cbk = getClientBucketAndKey(url);

    // We want the prefix to have a trailing slash
    String prefix = Strings.stripSuffix(cbk.key(), "/") + "/";

    ListObjectsRequest request = new ListObjectsRequest().withBucketName(cbk.bucket()).withPrefix(prefix).withDelimiter("/");

    ObjectListing listing = cbk.client.listObjects(request);

    if (listing.isTruncated()) {
      throw new RuntimeException("Truncated ObjectListing!  Making additional calls currently isn't implemented!");
    }

    Stream<String> keys = listing.getCommonPrefixes().stream();
    Stream<String> summaryKeys = listing.getObjectSummaries().stream().map(S3ObjectSummary::getKey);

    String urlWithSlash = Strings.stripSuffix(url.toString(), "/") + "/";
    Stream<URL> res = Stream.concat(keys, summaryKeys).map(k -> toURL(urlWithSlash + Strings.stripPrefix(k, prefix)));

    debug("list(" + url + ") => \n  " + res);

    return res.collect(Collectors.toList());
  }

  private URL toURL(String url) {
    try {
      return new URL(url);
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("RV_RETURN_VALUE_IGNORED_BAD_PRACTICE")
  public void download(URL src, File dest, CopyProgressListener l) {
    debug("download(" + src + ", " + dest + ")");

    ClientBucketKey cbk = getClientBucketAndKey(src);

    CopyProgressEvent event = new CopyProgressEvent();
    if (null != l) {
      l.start(event);
    }

    ObjectMetadata meta = cbk.client.getObject(new GetObjectRequest(cbk.bucket(), cbk.key()), dest);
    dest.setLastModified(meta.getLastModified().getTime());

    if (null != l) {
      l.end(event); //l.progress(evt.update(EMPTY_BUFFER, 0, meta.getContentLength))
    }
  }

  public void upload(File src, URL dest, CopyProgressListener l) {
    debug("upload(" + src + ", " + dest + ")");

    CopyProgressEvent event = new CopyProgressEvent();
    if (null != l) {
      l.start(event);
    }

    ClientBucketKey cbk = getClientBucketAndKey(dest);
    cbk.client.putObject(cbk.bucket(), cbk.key(), src);

    if (null != l) {
      l.end(event);
    }
  }

  // I don't think we care what this is set to
  public void setRequestMethod(int requestMethod) {
    debug("setRequestMethod(" + requestMethod + ")");
  }

  // Try to get the region of the S3 URL so we can set it on the S3Client
  private Optional<Region> getRegion(URL url, String bucket, AmazonS3Client client) {
    Optional<String> region = Optionals.first(
        () -> getRegionNameFromURL(url),
        () -> getRegionNameFromDNS(bucket),
        () -> getRegionNameFromService(bucket, client));
    return region.flatMap(r -> Optional.ofNullable(RegionUtils.getRegion(r)));
  }

  private Optional<String> getRegionNameFromURL(URL url) {
    // We'll try the AmazonS3URI parsing first then fallback to our RegionMatcher
    return Optionals.first(
        () -> getAmazonS3URI(url).flatMap(u -> Optional.ofNullable(u.getRegion())),
        () -> findFirstInRegionMatcher(url.toString()));
  }

  private Optional<String> getRegionNameFromDNS(String bucket) {
    try {
      // This gives us something like s3-us-west-2-w.amazonaws.com which must have changed
      // at some point because the region from that hostname is no longer parsed by AmazonS3URI
      String canonicalHostName = InetAddress.getByName(bucket + ".s3.amazonaws.com").getCanonicalHostName();

      // So we use our regex based RegionMatcher to try and extract the region since AmazonS3URI doesn't work
      return findFirstInRegionMatcher(canonicalHostName);
    } catch (UnknownHostException e) {
      throw new RuntimeException(e);
    }
  }

  private static Optional<String> findFirstInRegionMatcher(String str) {
    try {
      return Optional.ofNullable(RegionMatcher.matcher(str).group(1));
    } catch (IllegalStateException e) {
      return Optional.empty();
    }
  }

  // TODO: cache the result of this so we aren't always making the call
  private Optional<String> getRegionNameFromService(String bucket, AmazonS3Client client) {
    try {
      // This might fail if the current credentials don't have access to the getBucketLocation call
      return Optional.ofNullable(client.getBucketLocation(bucket));
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private BucketAndKey getBucketAndKey(URL url) {
    // The AmazonS3URI constructor should work for standard S3 urls.  But if a custom domain is being used
    // (e.g. snapshots.maven.frugalmechanic.com) then we treat the hostname as the bucket and the path as the key
    return getAmazonS3URI(url)
        .map(amzn -> new BucketAndKey(amzn.getBucket(), amzn.getKey()))
        .orElseGet(() -> new BucketAndKey(url.getHost(), Strings.stripPrefix(url.getPath(), "/")));
  }

  private Optional<AmazonS3URI> getAmazonS3URI(URL url) {
    try {
      return getAmazonS3URI(url.toURI());
    } catch (URISyntaxException e) {
      return Optional.empty();
    }
  }

  private Optional<AmazonS3URI> getAmazonS3URI(URI uri) {
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
