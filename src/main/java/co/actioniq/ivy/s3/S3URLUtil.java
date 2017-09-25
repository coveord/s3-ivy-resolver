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
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3URI;
import org.apache.ivy.util.Message;

import java.io.File;
import java.net.InetAddress;
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

class S3URLUtil {
  // This is for matching region names in URLs or host names
  private static final Pattern RegionMatcher = makeRegionMatcher();

  private Map<String,AWSCredentials> credentialsCache = new ConcurrentHashMap<>();
  private final String credentialFileName;

  S3URLUtil() {
    this("s3credentials");
  }

  S3URLUtil(String credentialFileName) {
    this.credentialFileName = credentialFileName;
  }

  ClientBucketKey getClientBucketAndKey(URL url) {
    BucketAndKey bk = getBucketAndKey(url);
    AmazonS3Client client = new AmazonS3Client(getCredentials(bk.bucket), getProxyConfiguration());
    Optional<Region> region = getRegion(url, bk.bucket, client);
    region.ifPresent(client::setRegion);
    return new ClientBucketKey(client, bk);
  }

  private static Pattern makeRegionMatcher() {
    Comparator<String> ReverseLengthComparator = Comparator.comparingInt(String::length).reversed();
    String pattern = Arrays.stream(Regions.values())
        .map(Regions::getName)
        .sorted(ReverseLengthComparator)
        .collect(Collectors.joining("|", "(", ")"));
    return Pattern.compile(pattern);
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

  // TODO: cache the result of this so we aren't always making the call
  private Optional<String> getRegionNameFromService(String bucket, AmazonS3Client client) {
    try {
      // This might fail if the current credentials don't have access to the getBucketLocation call
      return Optional.ofNullable(client.getBucketLocation(bucket));
    } catch (Exception e) {
      return Optional.empty();
    }
  }

  private static Optional<String> findFirstInRegionMatcher(String str) {
    try {
      return Optional.ofNullable(RegionMatcher.matcher(str).group(1));
    } catch (IllegalStateException e) {
      return Optional.empty();
    }
  }

  private Optional<AmazonS3URI> getAmazonS3URI(URL url) {
    // #coveo change: this causes callers to fall back on parsing the URL. For some reason getAmazonS3URI(URI) is broken...
//    try {
//      return getAmazonS3URI(url.toURI());
//    } catch (URISyntaxException e) {
    return Optional.empty();
//    }
  }

  private BucketAndKey getBucketAndKey(URL url) {
    // The AmazonS3URI constructor should work for standard S3 urls.  But if a custom domain is being used
    // (e.g. snapshots.maven.frugalmechanic.com) then we treat the hostname as the bucket and the path as the key
    return getAmazonS3URI(url)
        .map(amzn -> new BucketAndKey(amzn.getBucket(), amzn.getKey()))
        .orElseGet(() -> new BucketAndKey(url.getHost(), Strings.stripPrefix(url.getPath(), "/")));
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

  private AWSCredentials getCredentials(String bucket) {
    AWSCredentials credentials = credentialsCache.computeIfAbsent(bucket, this::computeCredentials);
    Message.debug("S3URLHandler - Using AWS Access Key Id: "+credentials.getAWSAccessKeyId()+" for bucket: "+bucket);
    return credentials;
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

  private AWSCredentials computeCredentials(String bucket) {
    try {
      return makeCredentialsProviderChain(bucket).getCredentials();
    } catch (AmazonClientException e) {
      Message.error("Unable to find AWS Credentials.");
      throw e;
    }
  }

  AWSCredentialsProviderChain makeCredentialsProviderChain(String bucket) {
    AWSCredentialsProvider[] basicProviders = {
        new BucketSpecificEnvironmentVariableCredentialsProvider(bucket),
        new BucketSpecificSystemPropertiesCredentialsProvider(bucket),
        makePropertiesFileCredentialsProvider("." + credentialFileName + "_" + bucket),
        makePropertiesFileCredentialsProvider("." + bucket + "_" + credentialFileName),
        new EnvironmentVariableCredentialsProvider(),
        new SystemPropertiesCredentialsProvider(),
        makePropertiesFileCredentialsProvider("." + credentialFileName),
        new ProfileCredentialsProvider(bucket), // #coveo change: check in .aws/credentials or .aws/config,
        new ProfileCredentialsProvider(),       // first using profile named after bucket, otherwise default profile
        new InstanceProfileCredentialsProvider()
    };

    AWSCredentialsProviderChain basicProviderChain = new AWSCredentialsProviderChain(basicProviders);

    AWSCredentialsProvider[] roleProviders = {
        new BucketSpecificRoleBasedEnvironmentVariableCredentialsProvider(basicProviderChain, bucket),
        new BucketSpecificRoleBasedSystemPropertiesCredentialsProvider(basicProviderChain, bucket),
        new RoleBasedPropertiesFileCredentialsProvider(basicProviderChain, "." + credentialFileName + "_" + bucket),
        new RoleBasedPropertiesFileCredentialsProvider(basicProviderChain, "." + bucket + "_" + credentialFileName),
        new RoleBasedEnvironmentVariableCredentialsProvider(basicProviderChain),
        new RoleBasedSystemPropertiesCredentialsProvider(basicProviderChain),
        new RoleBasedPropertiesFileCredentialsProvider(basicProviderChain, "." + credentialFileName)
    };

    List<AWSCredentialsProvider> providerList = new ArrayList<>();
    providerList.addAll(Arrays.asList(roleProviders));
    providerList.addAll(Arrays.asList(basicProviders));
    AWSCredentialsProvider[] providerArray = providerList.toArray(new AWSCredentialsProvider[providerList.size()]);

    return new AWSCredentialsProviderChain(providerArray);
  }

  private PropertiesFileCredentialsProvider makePropertiesFileCredentialsProvider(String fileName) {
    File file = new File(Constants.DotIvyDir, fileName);
    return new PropertiesFileCredentialsProvider(file.toString());
  }
}
