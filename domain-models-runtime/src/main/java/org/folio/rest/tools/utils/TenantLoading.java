package org.folio.rest.tools.utils;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.commons.io.IOUtils;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.util.StringUtil;

/**
 * TenantLoading is utility for loading data into modules during the Tenant Init
 * service.
 *
 * The loading is triggered by Tenant Init Parameters and the TenantLoading is
 * meant to be used in the implementation of the
 * {@link org.folio.rest.impl.TenantAPI#postTenantSync} method.
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
 * public void postTenant(TenantAttributes ta, Map<String, String> headers,
 *   Handler<AsyncResult<Response>> hndlr, Context cntxt) {
 *   Vertx vertx = cntxt.owner();
 *   super.postTenant(ta, headers, res -> {
 *     if (res.failed()) {
 *       hndlr.handle(res);
 *       return;
 *     }
 *     TenantLoading tl = new TenantLoading();
 *     tl.withKey("loadReference").withLead("ref-data")
 *     .add("groups")
 *     .withKey("loadSample").withLead("sample-data")
 *     .add("users")
 *     .perform(ta, headers, vertx, res1 -> {
 *       if (res1.failed()) {
 *         hndlr.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse
 *          .respond500WithTextPlain(res1.cause().getLocalizedMessage())));
 *        return;
 *       }
 *       hndlr.handle(io.vertx.core.Future.succeededFuture(PostTenantResponse
 *         .respond201WithApplicationJson("")));
 *     });
 *   }, cntxt);
 * }
 * </code>
 * </pre>
 *
 *
 */
public class TenantLoading {

  private static final Logger log = LoggerFactory.getLogger(TenantLoading.class);
  private static final String RETURNED_STATUS = " returned status ";

  private enum Strategy {
    CONTENT, // Id in JSON content PUT/POST
    BASENAME, // PUT with ID as basename
    RAW_PUT, // PUT with no ID
    RAW_POST, // POST with no ID
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

  private static void endWithXHeaders(HttpClientRequest req, Map<String, String> headers, String json) {
    for (Map.Entry<String, String> e : headers.entrySet()) {
      String k = e.getKey();
      if (k.startsWith("X-") || k.startsWith("x-")) {
        req.headers().add(k, e.getValue());
      }
    }
    req.headers().add("Content-Type", "application/json");
    req.headers().add("Accept", "application/json, text/plain");
    req.end(json);
  }

  static String getIdBase(String path, Future<Void> f) {
    int base = path.lastIndexOf('/');
    int suf = path.lastIndexOf('.');
    if (base == -1) {
      f.handle(Future.failedFuture("No basename for " + path));
      return null;
    }
    if (suf > base) {
      return path.substring(base, suf);
    } else {
      return path.substring(base);
    }
  }

  private static String getId(LoadingEntry loadingEntry, URL url, String content,
    Future<Void> f) {

    switch (loadingEntry.strategy) {
      case BASENAME:
        return getIdBase(url.getPath(), f);
      case CONTENT:
        JsonObject jsonObject = new JsonObject(content);
        String id = jsonObject.getString(loadingEntry.idProperty);
        if (id == null) {
          log.warn("Missing property "
            + loadingEntry.idProperty + " for url=" + url.toString());
          f.handle(Future.failedFuture("Missing property "
            + loadingEntry.idProperty + " for url=" + url.toString()));
          return null;
        }
        return StringUtil.urlEncode(id);
      case RAW_PUT:
      case RAW_POST:
        break;
    }
    return null;
  }

  private static void handleException(Throwable ex, String lead, Future<Void> f) {
    String diag = lead  + ": " + ex.getMessage();
    log.error(diag, ex);
    if (!f.isComplete()) {
      f.handle(Future.failedFuture(diag));
    }
  }

  private static void handleException(Throwable ex, HttpMethod method, String uri, Future<Void> f) {
    handleException(ex, method.name() + " " + uri, f);
  }

  private static String getContent(URL url, LoadingEntry loadingEntry, Future<Void> f) {
    try {
      String content = IOUtils.toString(url, StandardCharsets.UTF_8);
      if (loadingEntry.contentFilter != null) {
        return loadingEntry.contentFilter.apply(content);
      }
      return content;
    } catch (IOException ex) {
      handleException(ex, "IOException for url " + url.toString(), f);
      return null;
    }
  }

  private static void loadURL(Map<String, String> headers, URL url,
    HttpClient httpClient, LoadingEntry loadingEntry, String endPointUrl,
    Future<Void> f) {

    final String content = getContent(url, loadingEntry, f);
    if (f.isComplete()) {
      return;
    }
    String id = getId(loadingEntry, url, content, f);
    if (f.isComplete()) {
      return;
    }
    StringBuilder putUri = new StringBuilder();
    HttpMethod method1t;
    if (loadingEntry.strategy == Strategy.RAW_POST) {
      method1t = HttpMethod.POST;
    } else {
      method1t = HttpMethod.PUT;
    }
    if (id == null) {
      putUri.append(endPointUrl);
    } else {
      if (endPointUrl.contains("%d")) {
        putUri.append(endPointUrl.replaceAll("%d", id));
      } else {
        putUri.append(endPointUrl + "/" + id);
      }
    }
    final HttpMethod method1 = method1t;
    HttpClientRequest reqPut = httpClient.requestAbs(method1, putUri.toString(), resPut -> {
      Buffer body1 = Buffer.buffer();
      resPut.handler(body1::appendBuffer);
      resPut.endHandler(e -> {
        if (loadingEntry.strategy != Strategy.RAW_PUT
          && loadingEntry.strategy != Strategy.RAW_POST
          && (resPut.statusCode() == 404 || resPut.statusCode() == 400 || resPut.statusCode() == 422)) {
          HttpMethod method2 = HttpMethod.POST;
          HttpClientRequest reqPost = httpClient.requestAbs(method2, endPointUrl, resPost -> {
            Buffer body2 = Buffer.buffer();
            resPost.handler(body2::appendBuffer);
            resPost.endHandler(x -> {
              if (resPost.statusCode() == 201) {
                f.handle(Future.succeededFuture());
              } else {
                String diag = method1.name() + " " + putUri.toString()
                  + RETURNED_STATUS + resPut.statusCode() + ": " + body1.toString()
                  + " " + method2.name() + " " + endPointUrl
                  + RETURNED_STATUS + resPost.statusCode() + ": " + body2.toString();
                log.error(diag);
                f.handle(Future.failedFuture(diag));
              }
            });
          });
          reqPost.exceptionHandler(ex -> handleException(ex, method2, endPointUrl, f));
          endWithXHeaders(reqPost, headers, content);
        } else if (resPut.statusCode() == 200 || resPut.statusCode() == 201
          || resPut.statusCode() == 204 || loadingEntry.statusAccept.contains(resPut.statusCode())) {
          f.handle(Future.succeededFuture());
        } else {
          String diag = method1.name() + " " + putUri.toString() + RETURNED_STATUS + resPut.statusCode()
            + ": " + body1.toString();
          log.error(diag);
          f.handle(Future.failedFuture(diag));
        }
      });
      resPut.exceptionHandler(ex -> handleException(ex, method1, putUri.toString(), f));
    });
    reqPut.exceptionHandler(ex -> handleException(ex, method1, putUri.toString(), f));
    endWithXHeaders(reqPut, headers, content);
  }

  private static void loadData(String okapiUrl, Map<String, String> headers,
    LoadingEntry loadingEntry, HttpClient httpClient,
    Handler<AsyncResult<Integer>> res) {

    String filePath = loadingEntry.lead;
    if (!loadingEntry.filePath.isEmpty()) {
      filePath = filePath + '/' + loadingEntry.filePath;
    }
    final String endPointUrl = okapiUrl + "/" + loadingEntry.uriPath;
    try {
      List<URL> urls = getURLsFromClassPathDir(filePath);
      if (urls.isEmpty()) {
        log.warn("loadData getURLsFromClassPathDir returns empty list for path=" + filePath);
      }
      Future<Void> future = Future.succeededFuture();
      for (URL url : urls) {
        future = future.compose(x -> {
          Promise<Void> p = Promise.promise();
          loadURL(headers, url, httpClient, loadingEntry, endPointUrl, p.future());
          return p.future();
        });
      }
      future.onComplete(x -> {
        if (x.failed()) {
          res.handle(Future.failedFuture(x.cause().getLocalizedMessage()));
        } else {
          res.handle(Future.succeededFuture(urls.size()));
        }
      });
    } catch (URISyntaxException|IOException ex) {
      log.error("Exception for path " + filePath, ex);
      res.handle(Future.failedFuture("Exception for path " + filePath + " ex=" + ex.getMessage()));
    }
  }

  private void performR(String okapiUrl, TenantAttributes ta,
    Map<String, String> headers, Iterator<LoadingEntry> it,
    HttpClient httpClient, int number, Handler<AsyncResult<Integer>> res) {
    if (!it.hasNext()) {
      res.handle(Future.succeededFuture(number));
    } else {
      LoadingEntry le = it.next();
      if (ta != null) {
        for (Parameter parameter : ta.getParameters()) {
          if (le.key.equals(parameter.getKey()) && "true".equals(parameter.getValue())) {
            loadData(okapiUrl, headers, le, httpClient, x -> {
              if (x.failed()) {
                res.handle(Future.failedFuture(x.cause()));
              } else {
                performR(okapiUrl, ta, headers, it, httpClient, number + x.result(), res);
              }
            });
            return;
          }
        }
      }
      performR(okapiUrl, ta, headers, it, httpClient, number, res);
    }
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
   * @param vertx Vertx handle to be used (for spawning HTTP clients)
   * @param handler async result. If succesfull, the result is number of files
   * loaded.
   */
  public void perform(TenantAttributes ta, Map<String, String> headers,
    Vertx vertx, Handler<AsyncResult<Integer>> handler) {

    String okapiUrl = headers.get("X-Okapi-Url-to");
    if (okapiUrl == null) {
      log.warn("TenantLoading.perform No X-Okapi-Url-to header");
      okapiUrl = headers.get("X-Okapi-Url");
    }
    if (okapiUrl == null) {
      log.warn("TenantLoading.perform No X-Okapi-Url header");
      handler.handle(Future.failedFuture("No X-Okapi-Url header"));
      return;
    }
    Iterator<LoadingEntry> it = loadingEntries.iterator();
    HttpClient httpClient = vertx.createHttpClient();
    performR(okapiUrl, ta, headers, it, httpClient, 0, res -> {
      handler.handle(res);
      httpClient.close();
    });
  }

  /**
   * Specify for TenantLoading object the key that triggers loading of the
   * subsequent files to be added (see add method)
   *
   * For sample data, the convention is <literal>loadSample</literal>. For
   * reference data, the convention is <literal>loadReference</literal>.
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
   * This should be called prior to any add method In many cases files of same
   * type (eg sample) are all located in a leading directory. And the add method
   * will specify particular files under the leading directory.
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
   * In most cases, data has a unique key in JSON field <literal>"id"</literal>.
   * The content of the that field is used to check the existence of the object
   * or update thereof.
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
   * @param contentFilter filter that takes String as argument and returns
   * String
   * @return TenandLoading new state
   */
  public TenantLoading withFilter(UnaryOperator<String> contentFilter) {
    nextEntry.contentFilter = contentFilter;
    return this;
  }

  /**
   * Specify status code that will be accepted as "OK" beyond the normal ones
   *
   * By default for POST/PUT, 200,201,204 are considered OK. If you wish to
   * ignore a failure for POST (say of existing data), you can use this method.
   * You can repeat calls to it and the code added will be added to list of
   * accepted response codes.
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
   * In some cases, the identifier is not part of data, but instead given as
   * part of the filename that is holding the data to be posted. This method
   * handles that case.
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
   * Triggers PUT with raw path without unique id. The data presumably has an
   * identifier (but TenantLoading is not aware of what it is).
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
   * Triggers POST with raw path without unique id. The data presumably has an
   * identifier (but TenantLoading is not aware of what it is).
   *
   * @return TenandLoading new state
   */
  public TenantLoading withPostOnly() {
    nextEntry.strategy = Strategy.RAW_POST;
    return this;
  }

  /**
   * Adds a directory of files to be loaded (PUT/POST).
   *
   * @param filePath Relative directory path. Do not supply prefix or suffix
   * path separator (/) . The complete path is that of lead (withlead) followed
   * by this argument.
   * @param uriPath relative URI path. TenantLoading will add leading / and
   * combine with OkapiUrl.
   * @return TenantLoading new state
   */
  public TenantLoading add(String filePath, String uriPath) {
    nextEntry.filePath = filePath;
    nextEntry.uriPath = uriPath;
    loadingEntries.add(new LoadingEntry(nextEntry));
    return this;
  }

  /**
   * Adds a directory of files to be loaded (PUT/POST) This is a convenience
   * function that can be used when URI path and file path is the same.
   *
   * @param path URI path and File Path - when similar
   * @return TenandLoading new state
   */
  public TenantLoading add(String path) {
    return add(path, path);
  }

  /**
   * Adds files in directory with key, lead, Id content
   *
   * @param key Tenant Init parameter key (loadSample, loadPreference, ..)
   * @param lead Directory lead
   * @param filePath Directory below lead
   * @param uriPath URI path. Without leading /.
   * @deprecated Use withKey, withLead, withIdContent, add
   */
  @Deprecated
  public void addJsonIdContent(String key, String lead, String filePath,
    String uriPath) {
    withKey(key).withLead(lead).withIdContent().add(filePath, uriPath);
  }

  /**
   * Adds files in directory with key, lead, idBaseName
   *
   * @param key Tenant Init parameter key (loadSample, loadPreference, ..)
   * @param lead Directory lead
   * @param filePath Directory below lead
   * @param uriPath URI path. Without leading /.
   * @deprecated Use withKey, withLead, withIdBasename, add
   */
  @Deprecated
  public void addJsonIdBasename(String key, String lead, String filePath,
    String uriPath) {
    withKey(key).withLead(lead).withIdBasename().add(filePath, uriPath);
  }
}
