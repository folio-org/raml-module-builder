package org.folio.rest.tools.utils;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.UnaryOperator;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.commons.io.IOUtils;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.jaxrs.model.TenantAttributes;

public class TenantLoading {

  private static final Logger log = LoggerFactory.getLogger(TenantLoading.class);
  private static final String RETURNED_STATUS = " returned status ";
  private static final String FAILED_STR = " failed ";
  private static final String POST_STR = "POST ";
  private static final String PUT_STR = "PUT ";

  private enum Strategy {
    CONTENT, // Id in JSON content PUT/POST
    BASENAME, // PUT with ID as basename
    RAW  // PUT with no ID
  }

  private class LoadingEntry {

    String key;
    String lead;
    String filePath;
    String uriPath;
    String idProperty;
    private Strategy strategy;
    UnaryOperator<String> contentFilter;

    LoadingEntry(String key, String lead, String filePath, String uriPath, Strategy strategy,
      String idProperty, UnaryOperator<String> contentFilter) {
      this.key = key;
      this.lead = lead;
      this.filePath = filePath;
      this.uriPath = uriPath;
      this.strategy = strategy;
      this.idProperty = idProperty;
      this.contentFilter = contentFilter;
    }

    LoadingEntry() {
      this.strategy = Strategy.CONTENT;
      this.idProperty = "id";
      this.contentFilter = null;
    }
  }

  LoadingEntry nextEntry;

  List<LoadingEntry> loadingEntries;

  public TenantLoading() {
    loadingEntries = new LinkedList<>();
    nextEntry = new LoadingEntry();
  }

  protected static List<URL> getURLsFromClassPathDir(String directoryName)
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

  private static String getId(LoadingEntry loadingEntry, URL url, String content,
    Future<Void> f) {

    String id = null;
    switch (loadingEntry.strategy) {
      case BASENAME:
        int base = url.getPath().lastIndexOf(File.separator);
        int suf = url.getPath().lastIndexOf('.');
        if (base == -1) {
          f.handle(Future.failedFuture("No basename for " + url.toString()));
          return null;
        }
        if (suf > base) {
          id = url.getPath().substring(base, suf);
        } else {
          id = url.getPath().substring(base);
        }
        break;
      case CONTENT:
        JsonObject jsonObject = new JsonObject(content);
        id = jsonObject.getString(loadingEntry.idProperty);
        if (id == null) {
          log.warn("Missing property "
            + loadingEntry.idProperty + " for url=" + url.toString());

          f.handle(Future.failedFuture("Missing property "
            + loadingEntry.idProperty + " for url=" + url.toString()));
          return null;
        }
        try {
          id = URLEncoder.encode(id, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException ex) {
          f.handle(Future.failedFuture("Encoding of " + id + FAILED_STR));
          return null;
        }
        break;
      case RAW:
        break;
    }
    return id;
  }

  private static void loadURL(Map<String, String> headers, URL url,
    HttpClient httpClient, LoadingEntry loadingEntry, String endPointUrl,
    Future<Void> f) {

    log.info("loadURL url=" + url.toString());
    String content;
    try {
      InputStream stream = url.openStream();
      content = IOUtils.toString(stream, StandardCharsets.UTF_8);
      stream.close();
      if (loadingEntry.contentFilter != null) {
        content = loadingEntry.contentFilter.apply(content);
      }
    } catch (IOException ex) {
      f.handle(Future.failedFuture("IOException for url=" + url.toString() + " ex=" + ex.getLocalizedMessage()));
      return;
    }
    final String fContent = content;
    String id = getId(loadingEntry, url, content, f);
    if (f.isComplete()) {
      return;
    }
    StringBuilder putUri = new StringBuilder();
    if (id == null) {
      putUri.append(endPointUrl);
    } else {
      if (endPointUrl.contains("%d")) {
        putUri.append(endPointUrl.replaceAll("%d", id));
      } else {
        putUri.append(endPointUrl + "/" + id);
      }
    }
    HttpClientRequest reqPut = httpClient.putAbs(putUri.toString(), resPut -> {
      if (loadingEntry.strategy != Strategy.RAW
        && (resPut.statusCode() == 404 || resPut.statusCode() == 400)) {
        HttpClientRequest reqPost = httpClient.postAbs(endPointUrl, resPost -> {
          if (resPost.statusCode() == 201) {
            f.handle(Future.succeededFuture());
          } else {
            f.handle(Future.failedFuture(POST_STR + endPointUrl
              + RETURNED_STATUS + resPost.statusCode()));
          }
        });
        reqPost.exceptionHandler(ex -> {
          if (!f.isComplete()) {
            f.handle(Future.failedFuture(PUT_STR + putUri.toString()
              + ": " + ex.getMessage()));
          }
          log.warn(POST_STR + endPointUrl + ": " + ex.getMessage());
        });
        endWithXHeaders(reqPost, headers, fContent);
      } else if (resPut.statusCode() == 200 || resPut.statusCode() == 204) {
        f.handle(Future.succeededFuture());
      } else {
        log.warn(PUT_STR + putUri.toString() + RETURNED_STATUS + resPut.statusCode());
        f.handle(Future.failedFuture(PUT_STR + putUri.toString()
          + RETURNED_STATUS + resPut.statusCode()));
      }
    });
    reqPut.exceptionHandler(ex -> {
      if (!f.isComplete()) {
        f.handle(Future.failedFuture(PUT_STR + putUri.toString()
          + ": " + ex.getMessage()));
      }
      log.warn(PUT_STR + putUri.toString() + ": " + ex.getMessage());
    });
    endWithXHeaders(reqPut, headers, content);
  }

  private static void loadData(String okapiUrl, Map<String, String> headers,
    LoadingEntry loadingEntry, HttpClient httpClient,
    Handler<AsyncResult<Integer>> res) {

    final String filePath = loadingEntry.lead + File.separator + loadingEntry.filePath;
    log.info("loadData uriPath=" + loadingEntry.uriPath + " filePath=" + filePath);
    final String endPointUrl = okapiUrl + "/" + loadingEntry.uriPath;
    List<Future> futures = new LinkedList<>();
    try {
      List<URL> urls = getURLsFromClassPathDir(filePath);
      if (urls.isEmpty()) {
        log.info("loadData getURLsFromClassPathDir returns empty list");
      }
      for (URL url : urls) {
        Future<Void> f = Future.future();
        futures.add(f);
        loadURL(headers, url, httpClient, loadingEntry, endPointUrl, f);
      }
      CompositeFuture.all(futures).setHandler(x -> {
        if (x.failed()) {
          res.handle(Future.failedFuture(x.cause().getLocalizedMessage()));
        } else {
          res.handle(Future.succeededFuture(urls.size()));
        }
      });
    } catch (URISyntaxException ex) {
      res.handle(Future.failedFuture("URISyntaxException for path " + filePath + " ex=" + ex.getLocalizedMessage()));
    } catch (IOException ex) {
      res.handle(Future.failedFuture("IOException for path " + filePath + " ex=" + ex.getLocalizedMessage()));
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

  public TenantLoading withKey(String key) {
    nextEntry.key = key;
    return this;
  }

  public TenantLoading withLead(String lead) {
    nextEntry.lead = lead;
    return this;
  }

  public TenantLoading withIdContent() {
    nextEntry.idProperty = "id";
    nextEntry.strategy = Strategy.CONTENT;
    return this;
  }

  public TenantLoading withContent(String idProperty) {
    nextEntry.idProperty = idProperty;
    nextEntry.strategy = Strategy.CONTENT;
    return this;
  }

  public TenantLoading withFilter(UnaryOperator<String> contentFilter) {
    nextEntry.contentFilter = contentFilter;
    return this;
  }

  public TenantLoading withIdBasename() {
    nextEntry.strategy = Strategy.BASENAME;
    return this;
  }

  public TenantLoading withIdRaw() {
    nextEntry.strategy = Strategy.RAW;
    return this;
  }

  public TenantLoading add(String filePath, String uriPath) {
    loadingEntries.add(new LoadingEntry(nextEntry.key, nextEntry.lead,
      filePath, uriPath, nextEntry.strategy, nextEntry.idProperty,
      nextEntry.contentFilter));
    return this;
  }

  public TenantLoading add(String path) {
    return add(path, path);
  }

  public void addJsonIdContent(String key, String lead, String filePath,
    String uriPath) {
    withKey(key).withLead(lead).withIdContent().add(filePath, uriPath);
  }

  public void addJsonIdBasename(String key, String lead, String filePath,
    String uriPath) {
    withKey(key).withLead(lead).withIdBasename().add(filePath, uriPath);
  }
}
