package org.folio.rest.tools.utils;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.okapi.common.GenericCompositeFuture;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.util.PercentCodec;

/**
 * TenantLoading is utility for loading data into modules during the Tenant Init service.
 *
 * The loading is triggered by Tenant Init Parameters and the TenantLoading is
 * meant to be used in the implementation of the
 * {@link org.folio.rest.impl.TenantAPI#loadData} method.
 *
 * Different strategies for communicating with the web service
 * <ul>
 * <li>{@link #withIdContent} / {@link #withContent} TenantLoading retrieves
 * unique identifier from JSON content so that it can perform PUT/POST/GET
 * operations
 * </li>
 * <li>{@link #withIdBasename} TenantLoading retrieves unique identifier from
 * basename of file to perform PUT/POST/GET operations
 * </li>
 * <li>{@link #withIdRaw} / {@link #withPostOnly} TenantLoading is unaware of
 * identifier and, can, thus only perform PUT / POST .
 * </li>
 * </ul>
 *
 * <pre>
 * <code>
 *
 * @Override
 * Future<Void> loadData(TenantAttributes attributes, String tenantId,
 *                       Map<String, String> headers, Context vertxContext) {
 *   return super.loadData(attributes, tenantId, headers, vertxContext)
 *       .compose(res -> new TenantLoading()
 *           .withKey("loadReference").withLead("ref-data")
 *           .add("groups")
 *           .withKey("loadSample").withLead("sample-data")
 *           .add("users")
 *           .perform(attributes, headers, vertxContext));
 * }
 *
 * </code>
 * </pre>
 */
public class TenantLoading {

  private static final Logger log = LogManager.getLogger(TenantLoading.class);
  private static final String RETURNED_STATUS = " returned status ";

  private enum Strategy {
    CONTENT, // Id in JSON content PUT/POST
    BASENAME, // PUT with ID as basename
    RAW_PUT, // PUT with no ID
    RAW_POST, // POST with no ID
    RAW_POST_IGNORE, // POST with no ID, ignore 4xx error (already exists, etc.)
  }

  private class LoadingEntry {

    UnaryOperator<String> contentFilter;
    Set<Integer> statusAccept;
    String key;
    String lead;
    String filePath;
    String uriPath;
    String idProperty;
    private Strategy strategy;

    LoadingEntry(LoadingEntry le) {
      this.key = le.key;
      this.lead = le.lead;
      this.filePath = le.filePath;
      this.uriPath = le.uriPath;
      this.strategy = le.strategy;
      this.idProperty = le.idProperty;
      this.contentFilter = le.contentFilter;
      this.statusAccept = le.statusAccept;
    }

    LoadingEntry() {
      this.strategy = Strategy.CONTENT;
      this.idProperty = "id";
      this.contentFilter = null;
      this.statusAccept = new HashSet<>();
    }
  }

  LoadingEntry nextEntry;

  List<LoadingEntry> loadingEntries;

  public TenantLoading() {
    loadingEntries = new LinkedList<>();
    nextEntry = new LoadingEntry();
  }

  /**
   * Get URLs for files in path (resources)
   *
   * @param directoryName (no prefix or suffix )
   * @return list of URLs
   * @throws URISyntaxException
   * @throws IOException
   */
  public static List<URL> getURLsFromClassPathDir(String directoryName)
      throws URISyntaxException, IOException {

    List<URL> filenames = new LinkedList<>();
    URL url = Thread.currentThread().getContextClassLoader().getResource(directoryName);
    if (url != null) {
      if (url.getProtocol().equals("file")) {
        File file = Paths.get(url.toURI()).toFile();
        if (file != null) {
          File[] files = file.listFiles();
          if (files != null) {
            for (File filename : files) {
              URL resource = filename.toURI().toURL();
              filenames.add(resource);
            }
          }
        }
      } else if (url.getProtocol().equals("jar")) {
        String dirname = directoryName + "/";
        String path = url.getPath();
        String jarPath = path.substring(5, path.indexOf('!'));
        try (JarFile jar = new JarFile(URLDecoder.decode(jarPath, StandardCharsets.UTF_8.name()))) {
          Enumeration<JarEntry> entries = jar.entries();
          while (entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            String name = entry.getName();
            if (name.startsWith(dirname) && !dirname.equals(name)) {
              URL resource = Thread.currentThread().getContextClassLoader().getResource(name);
              filenames.add(resource);
            }
          }
        }
      }
    }
    return filenames;
  }

  private static Future<HttpResponse<Buffer>> sendWithXHeaders(HttpRequest<Buffer> req, Map<String, String> headers,
      String json) {
    for (Map.Entry<String, String> e : headers.entrySet()) {
      String k = e.getKey();
      if (k.startsWith("X-") || k.startsWith("x-")) {
        req.headers().add(k, e.getValue());
      }
    }
    req.headers().add("Content-Type", "application/json");
    req.headers().add("Accept", "application/json, text/plain");
    if (json == null) {
      return req.send();
    }
    return req.sendBuffer(Buffer.buffer(json));
  }

  static Future<String> getIdBase(String path) {
    int base = path.lastIndexOf('/');
    int suf = path.lastIndexOf('.');
    if (base == -1) {
      return Future.failedFuture("No basename for " + path);
    }
    if (suf > base) {
      return Future.succeededFuture(path.substring(base, suf));
    } else {
      return Future.succeededFuture(path.substring(base));
    }
  }

  private static Future<String> getId(LoadingEntry loadingEntry, URL url, String content) {
    switch (loadingEntry.strategy) {
      case BASENAME:
        return getIdBase(url.getPath());
      case CONTENT:
        JsonObject jsonObject = new JsonObject(content);
        String id = jsonObject.getString(loadingEntry.idProperty);
        if (id == null) {
          String msg = "Missing property " + loadingEntry.idProperty + " for url=" + url;
          log.warn(msg);
          return Future.failedFuture(msg);
        }
        return Future.succeededFuture(PercentCodec.encodeAsString(id));
      case RAW_PUT:
      case RAW_POST:
      case RAW_POST_IGNORE:
        break;
    }
    return Future.succeededFuture(null);
  }

  private static String handleException(Throwable ex, String lead) {
    String diag = lead + ": " + ex.getMessage();
    log.error(diag, ex);
    return diag;
  }

  static Future<String> getContent(URL url, LoadingEntry loadingEntry) {
    try {
      String content = IOUtils.toString(url, StandardCharsets.UTF_8);
      if (loadingEntry.contentFilter != null) {
        return Future.succeededFuture(loadingEntry.contentFilter.apply(content));
      }
      return Future.succeededFuture(content);
    } catch (IOException ex) {
      return Future.failedFuture(handleException(ex, "IOException for url " + url.toString()));
    }
  }

  private Future<Integer> loadURL(Map<String, String> headers, URL url,
                                      WebClient httpClient, LoadingEntry loadingEntry, String endPointUrl) {

    return getContent(url, loadingEntry)
        .compose(content -> getId(loadingEntry, url, content)
            .compose(id -> loadURL(headers, content, id, httpClient, loadingEntry, endPointUrl)));
  }

  private static boolean isSuccess(HttpResponse<Buffer> httpResponse, LoadingEntry loadingEntry) {
    return
        httpResponse.statusCode() == 200 ||
        httpResponse.statusCode() == 201 ||
        httpResponse.statusCode() == 204 ||
        loadingEntry.statusAccept.contains(httpResponse.statusCode());
  }

  /**
   * Content with _version copied from the body of httpResponse if httpMethod is PUT and the body
   * has a _version property. Otherwise content unchanged.
   */
  private static String mergeVersion(HttpMethod httpMethod, String content, HttpResponse<Buffer> httpResponse) {
    if (httpMethod != HttpMethod.PUT) {
      return content;
    }
    Integer version = httpResponse.bodyAsJsonObject().getInteger("_version");
    if (version == null) {
      return content;
    }
    return new JsonObject(content).put("_version", version).encode();
  }

  /**
   * Returns 1 on successful POST/PUT, 0 on 4xx HTTP status if strategy is POST_IGNORE
   */
  private Future<Integer> loadURL(Map<String, String> headers, String content, String id,
                                      WebClient httpClient, LoadingEntry loadingEntry, String endPointUrl) {

    if (loadingEntry.strategy == Strategy.RAW_POST_IGNORE) {
      HttpRequest<Buffer> req = httpClient.postAbs(endPointUrl);
      return sendWithXHeaders(req, headers, content)
          .compose(res -> {
            if (isSuccess(res, loadingEntry)) {
              return Future.succeededFuture(1);
            }
            // ignore 4xx errors
            if (400 <= res.statusCode() && res.statusCode() <= 499) {
              return Future.succeededFuture(0);
            }
            String diag = "POST " + endPointUrl + RETURNED_STATUS
                + res.statusCode() + ": " + res.bodyAsString();
            log.error(diag);
            return Future.failedFuture(diag);
          });
    }
    StringBuilder idUrl = new StringBuilder();
    HttpMethod method1 = (loadingEntry.strategy == Strategy.RAW_POST) ? HttpMethod.POST : HttpMethod.PUT;
    if (id == null) {
      idUrl.append(endPointUrl);
    } else {
      if (endPointUrl.contains("/%d")) {
        idUrl.append(endPointUrl.replaceAll("/%d", id));
      } else {
        idUrl.append(endPointUrl + "/" + id);
      }
    }
    if (loadingEntry.strategy == Strategy.RAW_PUT ||
        loadingEntry.strategy == Strategy.RAW_POST) {
      String url = loadingEntry.strategy == Strategy.RAW_PUT ? idUrl.toString() : endPointUrl;
      HttpRequest<Buffer> req = httpClient.requestAbs(method1, url);
      return sendWithXHeaders(req, headers, content)
          .compose(res -> {
            if (isSuccess(res, loadingEntry)) {
              return Future.succeededFuture(1);
            }
            String diag = method1.name() + " " + url + RETURNED_STATUS
                + res.statusCode() + ": " + res.bodyAsString();
            log.error(diag);
            return Future.failedFuture(diag);
          });
    }

    return getWithRetry(idUrl.toString(), headers, httpClient).compose(resGet -> {
      HttpMethod method2 = (resGet.statusCode() == 200) ? HttpMethod.PUT : HttpMethod.POST;
      String url2 = (method2 == HttpMethod.PUT) ? idUrl.toString() : endPointUrl.replace("/%d", "");
      String finalContent = mergeVersion(method2, content, resGet);
      HttpRequest<Buffer> req2 = httpClient.requestAbs(method2, url2);
      return sendWithXHeaders(req2, headers, finalContent).compose(res2 -> {
        if (res2.statusCode() == 201 || res2.statusCode() == 204) {
          return Future.succeededFuture(1);
        }
        String diag = "GET " + idUrl.toString()
          + RETURNED_STATUS + resGet.statusCode() + ": " + resGet.bodyAsString()
          + "; " + method2.name() + " " + url2
          + RETURNED_STATUS + res2.statusCode() + ": " + res2.bodyAsString();
        log.error(diag);
        return Future.failedFuture(diag);
      });
    });
  }

  /**
   * Execute GET url up to four times until the response code if 200 or 404,
   * otherwise return failed Future.
   */
  Future<HttpResponse<Buffer>> getWithRetry(String url, Map<String,String> headers, WebClient webClient) {
    return get(url, headers, webClient)
        .recover(e -> sleep(1000).compose(x -> get(url, headers, webClient)))
        .recover(e -> sleep(5000).compose(x -> get(url, headers, webClient)))
        .recover(e -> sleep(10000).compose(x -> get(url, headers, webClient)));
  }

  /**
   * Sleep a random time of the range millisMin to 2*millisMin.
   *
   * @param millisMin the minimum time in milliseconds to sleep
   */
  @SuppressWarnings("java:S2245")  // Using pseudorandom number generator Math.random() for sleep time is safe
  Future<Void> sleep(long millisMin) {
    long millis = (long) (millisMin + Math.random() * millisMin);
    Vertx vertx = VertxUtils.getVertxFromContextOrNew();
    return Future.future(promise -> vertx.setTimer(millis, x -> promise.complete()));
  }

  /**
   * Execute GET url, return response if status code is 200 or 404, otherwise return failed Future.
   */
  private static Future<HttpResponse<Buffer>> get(String url, Map<String,String> headers, WebClient webClient) {
    return sendWithXHeaders(webClient.getAbs(url), headers, null)
        .compose(response -> {
          switch (response.statusCode()) {
            case 200:
            case 404:
              return Future.succeededFuture(response);
            default:
              return Future.failedFuture(url + ": " + response.statusCode() + " " + response.bodyAsString());
          }
        });
  }

  private Future<Integer> loadData(String okapiUrl, Map<String, String> headers,
      LoadingEntry loadingEntry, WebClient httpClient) {

    String filePath = loadingEntry.lead;
    if (!loadingEntry.filePath.isEmpty()) {
      filePath = filePath + '/' + loadingEntry.filePath;
    }
    final String endPointUrl = okapiUrl + "/" + loadingEntry.uriPath;
    try {
      List<URL> urls = getURLsFromClassPathDir(filePath);
      if (urls.isEmpty()) {
        log.warn("loadData getURLsFromClassPathDir returns empty list for path=" + filePath);
        return Future.succeededFuture(0);
      }
      AtomicInteger sum = new AtomicInteger(0);
      List<Future<Integer>> futures = new ArrayList<>(urls.size());
      urls.forEach(url -> futures.add(
          loadURL(headers, url, httpClient, loadingEntry, endPointUrl)
          .onSuccess(sum::addAndGet)));
      return GenericCompositeFuture.all(futures).map(x -> sum.get());
    } catch (URISyntaxException|IOException ex) {
      log.error("Exception for path " + filePath, ex);
      return Future.failedFuture("Exception for path " + filePath + " ex=" + ex.getMessage());
    }
  }

  private Future<Integer> perform0(TenantAttributes ta, Map<String, String> headers,
                                   Vertx vertx, int recordsLoaded) {

    String okapiUrl = headers.get(XOkapiHeaders.URL_TO);
    if (okapiUrl == null) {
      log.warn("TenantLoading.perform No " + XOkapiHeaders.URL_TO + " header");
      okapiUrl = headers.get(XOkapiHeaders.URL);
    }
    if (okapiUrl == null) {
      log.warn("TenantLoading.perform No " + XOkapiHeaders.URL + " header");
      return Future.failedFuture("No " + XOkapiHeaders.URL + " header");
    }
    WebClient httpClient = WebClient.create(vertx);
    Future<Integer> future = Future.succeededFuture(recordsLoaded);
    for (LoadingEntry entry : loadingEntries) {
      if (ta != null) {
        final String okapiUrlFinal = okapiUrl;
        for (Parameter parameter : ta.getParameters()) {
          if (entry.key.equals(parameter.getKey()) && "true".equals(parameter.getValue())) {
            future = future.compose(sum -> loadData(okapiUrlFinal, headers, entry, httpClient)
                .map(newRecords -> sum + newRecords));
          }
        }
      }
    }
    return future.onComplete(complete -> httpClient.close());
}

  /**
   * Perform the actual loading of files
   *
   * This is normally the last method to be executed for the TenantLoading
   * instance.
   *
   * See {@link TenantLoading} for an example.
   *
   * @param ta Tenant Attributes as they are passed via Okapi install
   * @param headers Okapi headers taken verbatim from RMBs handler
   * @param context Vert.x context
   * @param recordsLoaded number of records that have already been loaded
   * @return async result with total number of records loaded (sum of recordsLoaded and the load of this perform).
   */
  public Future<Integer> perform(TenantAttributes ta, Map<String, String> headers,
      Context context, int recordsLoaded) {
    return perform0(ta, headers, context.owner(), recordsLoaded);
  }

  /**
   * Perform the actual loading of files
   *
   * This is normally the last method to be executed for the TenantLoading instance.
   *
   * See {@link TenantLoading} for an example.
   *
   * @param ta Tenant Attributes as they are passed via Okapi install
   * @param headers Okapi headers taken verbatim from RMBs handler
   * @param vertx Vertx handle to be used (for spawning HTTP clients)
   * @param handler async result. If successful, the result is number of records loaded by this perform.
   * loaded.
   */
  public void perform(TenantAttributes ta, Map<String, String> headers,
      Vertx vertx, Handler<AsyncResult<Integer>> handler) {
    perform0(ta, headers, vertx, 0).onComplete(handler::handle);
  }

  /**
   * Specify for TenantLoading object the key that triggers loading of the subsequent files to be
   * added (see add method)
   *
   * For sample data, the convention is <literal>loadSample</literal>. For reference data, the
   * convention is <literal>loadReference</literal>.
   *
   * @param key the parameter key
   * @return TenandLoading new state
   */
  public TenantLoading withKey(String key) {
    nextEntry.key = key;
    return this;
  }

  /**
   * Specify the leading directory of files
   *
   * This should be called prior to any add method In many cases files of same type (eg sample) are
   * all located in a leading directory. And the add method will specify particular files under the
   * leading directory.
   *
   * @param lead the leading directory (without suffix of prefix separator)
   * @return TenandLoading new state
   */
  public TenantLoading withLead(String lead) {
    nextEntry.lead = lead;
    return this;
  }

  /**
   * Specify loading with unique key in JSON field "id"
   *
   * In most cases, data has a unique key in JSON field <literal>"id"</literal>. The content of the
   * that field is used to check the existence of the object or update thereof.
   *
   * @return TenandLoading new state
   */
  public TenantLoading withIdContent() {
    nextEntry.idProperty = "id";
    nextEntry.strategy = Strategy.CONTENT;
    return this;
  }

  /**
   * Specify loading with unique key in custom JSON field
   *
   * Should be used if unique key is in other field than
   * <literal>"id"</literal>. The content of the that field is used to check the
   * existence of the object or update thereof.
   *
   * @return TenandLoading new state
   */
  public TenantLoading withContent(String idProperty) {
    nextEntry.idProperty = idProperty;
    nextEntry.strategy = Strategy.CONTENT;
    return this;
  }

  /**
   * Specify transform of data (before loading)
   *
   * Optional filter that can be specified to modify content before loading
   *
   * @param contentFilter filter that takes String as argument and returns String
   * @return TenandLoading new state
   */
  public TenantLoading withFilter(UnaryOperator<String> contentFilter) {
    nextEntry.contentFilter = contentFilter;
    return this;
  }

  /**
   * Specify status code that will be accepted as "OK" beyond the normal ones
   *
   * By default for POST/PUT, 200,201,204 are considered OK. If you wish to ignore a failure for
   * POST (say of existing data), you can use this method. You can repeat calls to it and the code
   * added will be added to list of accepted response codes.
   *
   * @param code The HTTP status code that is considered accepted (OK)
   * @return TenandLoading new state
   */
  public TenantLoading withAcceptStatus(int code) {
    nextEntry.statusAccept.add(code);
    return this;
  }

  /**
   * Specify that unique identifier is part of filename, rather than content
   *
   * In some cases, the identifier is not part of data, but instead given as part of the filename
   * that is holding the data to be posted. This method handles that case.
   *
   * @return TenandLoading new state
   */
  public TenantLoading withIdBasename() {
    nextEntry.strategy = Strategy.BASENAME;
    return this;
  }

  /**
   * Specify PUT without unique id in data
   *
   * Triggers PUT with raw path without unique id. The data presumably has an identifier (but
   * TenantLoading is not aware of what it is).
   *
   * @return TenandLoading new state
   */
  public TenantLoading withIdRaw() {
    nextEntry.strategy = Strategy.RAW_PUT;
    return this;
  }

  /**
   * Specify POST without unique id in data
   *
   * Triggers POST with raw path without unique id. The data presumably has an identifier (but
   * TenantLoading is not aware of what it is).
   *
   * @return TenandLoading new state
   */
  public TenantLoading withPostOnly() {
    nextEntry.strategy = Strategy.RAW_POST;
    return this;
  }

  /**
   * Specify POST, ignoring any 4xx HTTP status error, for example if record already exists.
   *
   * Triggers POST with raw path without unique id. The data presumably has an identifier (but
   * TenantLoading is not aware of what it is).
   *
   * @return TenandLoading new state
   */
  public TenantLoading withPostIgnore() {
    nextEntry.strategy = Strategy.RAW_POST_IGNORE;
    return this;
  }

  /**
   * Adds a directory of files to be loaded (PUT/POST).
   *
   * @param filePath Relative directory path. Do not supply prefix or suffix path separator (/) .
   * The complete path is that of lead (withlead) followed by this argument.
   * @param uriPath relative URI path. TenantLoading will add leading / and combine with OkapiUrl.
   * @return TenantLoading new state
   */
  public TenantLoading add(String filePath, String uriPath) {
    nextEntry.filePath = filePath;
    nextEntry.uriPath = uriPath;
    loadingEntries.add(new LoadingEntry(nextEntry));
    return this;
  }

  /**
   * Adds a directory of files to be loaded (PUT/POST) This is a convenience function that can be
   * used when URI path and file path is the same.
   *
   * @param path URI path and File Path - when similar
   * @return TenandLoading new state
   */
  public TenantLoading add(String path) {
    return add(path, path);
  }
}
