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
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

class S3URLHandler implements URLHandler {
  // One time setup to register our handler for S3:// urls in Ivy
  private static final boolean _init = initHandlers();

  private S3URLUtil s3URLUtil = new S3URLUtil();

  private static boolean initHandlers() {
    initDispatcher();
    initStreamHandler();
    return true;
  }

  private static void initDispatcher() {
    URLHandler defaultHandler = URLHandlerRegistry.getDefault();
    URLHandlerDispatcher dispatcher;
    if (defaultHandler instanceof URLHandlerDispatcher) {
      debug("Using the existing Ivy URLHandlerDispatcher to handle s3:// URLs");
      dispatcher = (URLHandlerDispatcher)defaultHandler;
    } else {
      debug("Creating a new Ivy URLHandlerDispatcher to handle s3:// URLs");
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
      debug("The s3:// URLStreamHandler is already installed");
    } catch (MalformedURLException e) {
      // This means we haven't installed the handler, so install it
        debug("Installing the s3:// URLStreamHandler via java.net.URL.setURLStreamHandlerFactory");
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

  public URLInfo getURLInfo(URL url, int timeout) {
    try {
      debug("getURLInfo(" + url + ", " + timeout + ")");

      ClientBucketKey cbk = s3URLUtil.getClientBucketAndKey(url);

      ObjectMetadata meta;
      try {
        meta = cbk.getObjectMetadata(cbk.bucket(), cbk.key());
      } catch (AmazonS3Exception e) {
        cbk = s3URLUtil.getNewClientBucketAndKey(url);
        meta = cbk.getObjectMetadata(cbk.bucket(), cbk.key());
      }

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

    ClientBucketKey cbk = s3URLUtil.getClientBucketAndKey(url);
    S3Object obj;
    try {
      obj = cbk.getObject(cbk.bucket(), cbk.key());
    } catch (AmazonS3Exception e) {
      cbk = s3URLUtil.getNewClientBucketAndKey(url);
      obj = cbk.getObject(cbk.bucket(), cbk.key());
    }
    return obj.getObjectContent();
  }

  /**
   * A directory listing for keys/directories under this prefix
   */
  List<URL> list(URL url) throws MalformedURLException {
    debug("list(" + url + ")");

      /* key is the prefix in this case */
    ClientBucketKey cbk = s3URLUtil.getClientBucketAndKey(url);

    // We want the prefix to have a trailing slash
    String prefix = Strings.stripSuffix(cbk.key(), "/") + "/";

    ListObjectsRequest request = new ListObjectsRequest().withBucketName(cbk.bucket()).withPrefix(prefix).withDelimiter("/");

    ObjectListing listing;
    try {
      listing = cbk.listObjects(request);
    } catch (AmazonS3Exception e) {
      cbk = s3URLUtil.getNewClientBucketAndKey(url);
      listing = cbk.listObjects(request);
    }

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

    ClientBucketKey cbk = s3URLUtil.getClientBucketAndKey(src);

    CopyProgressEvent event = new CopyProgressEvent();
    if (null != l) {
      l.start(event);
    }

    ObjectMetadata meta;
    try {
      meta = cbk.getObject(new GetObjectRequest(cbk.bucket(), cbk.key()), dest);
    } catch (AmazonS3Exception e) {
      cbk = s3URLUtil.getNewClientBucketAndKey(src);
      meta = cbk.getObject(new GetObjectRequest(cbk.bucket(), cbk.key()), dest);
    }
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

    ClientBucketKey cbk = s3URLUtil.getClientBucketAndKey(dest);
    try {
      cbk.putObject(cbk.bucket(), cbk.key(), src);
    } catch (AmazonS3Exception e) {
      cbk = s3URLUtil.getNewClientBucketAndKey(dest);
      cbk.putObject(cbk.bucket(), cbk.key(), src);
    }

    if (null != l) {
      l.end(event);
    }
  }

  // I don't think we care what this is set to
  public void setRequestMethod(int requestMethod) {
    debug("setRequestMethod(" + requestMethod + ")");
  }
}
