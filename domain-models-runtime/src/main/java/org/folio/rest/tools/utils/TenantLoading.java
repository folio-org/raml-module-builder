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
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.apache.commons.io.IOUtils;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.jaxrs.model.TenantAttributes;

public class TenantLoading {

  private static final Logger log = LoggerFactory.getLogger(TenantLoading.class);

  private class LoadingEntry {

    String key;
    String lead;
    String filePath;
    String uriPath;
    boolean useBasename;

    LoadingEntry(String key, String lead, String filePath, String uriPath) {
      this.key = key;
      this.lead = lead;
      this.filePath = filePath;
      this.uriPath = uriPath;
      this.useBasename = false;
    }
  }

  List<LoadingEntry> loadingEntries;

  public TenantLoading() {
    loadingEntries = new LinkedList<>();
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

  private static void loadURL(Map<String, String> headers, URL url,
    HttpClient httpClient, boolean useBasename, String endPointUrl,
    Future<Void> f) throws IOException {

    log.info("loadURL url=" + url.toString());
    InputStream stream = url.openStream();
    String content = IOUtils.toString(stream, StandardCharsets.UTF_8);
    String id;
    if (useBasename) {
      int base = url.getPath().lastIndexOf(File.separator);
      int suf = url.getPath().lastIndexOf('.');
      if (base == -1) {
        f.handle(Future.failedFuture("No basename for " + url.toString()));
        return;
      }
      if (suf > base) {
        id = url.getPath().substring(base, suf);
      } else {
        id = url.getPath().substring(base);
      }
    } else {
      JsonObject jsonObject = new JsonObject(content);
      id = jsonObject.getString("id");
      if (id == null) {
        f.handle(Future.failedFuture("Missing id for " + content));
        return;
      }
    }
    StringBuilder putUri = new StringBuilder();
    if (endPointUrl.contains("%d")) {
      putUri.append(endPointUrl.replaceAll("%d", id));
    } else {
      putUri.append(endPointUrl + "/" + id);
    }
    HttpClientRequest reqPut = httpClient.putAbs(putUri.toString(), resPut -> {
      if (resPut.statusCode() == 404 || resPut.statusCode() == 400) {
        HttpClientRequest reqPost = httpClient.postAbs(endPointUrl, resPost -> {
          resPost.endHandler(x -> {
            if (resPost.statusCode() == 201) {
              f.handle(Future.succeededFuture());
            } else {
              f.handle(Future.failedFuture("POST " + endPointUrl + " returned status " + resPost.statusCode()));
            }
          });
        });
        reqPost.exceptionHandler(x
          -> {
          log.warn("POST " + endPointUrl + " failed");
          f.handle(Future.failedFuture("POST " + endPointUrl + " failed"));
        }
        );
        endWithXHeaders(reqPost, headers, content);
      } else if (resPut.statusCode() == 200 || resPut.statusCode() == 204) {
        f.handle(Future.succeededFuture());
      } else {
        log.warn("PUT " + putUri.toString() + " returned status " + resPut.statusCode());
        f.handle(Future.failedFuture("PUT " + putUri.toString() + " returned status " + resPut.statusCode()));
      }
    });
    reqPut.exceptionHandler(x
      -> {
      log.warn("PUT " + putUri.toString() + " failed");
      f.handle(Future.failedFuture("PUT " + putUri.toString() + " failed"));
    });
    endWithXHeaders(reqPut, headers, content);
  }

  private static void loadData(Map<String, String> headers,
    String filePath, String uriPath, boolean useBasename,
    HttpClient httpClient, Handler<AsyncResult<Integer>> res) {

    log.info("loadData uriPath=" + uriPath + " filePath=" + filePath);
    String okapiUrl = headers.get("X-Okapi-Url-to");
    if (okapiUrl == null) {
      log.warn("loadData No X-Okapi-Url-to header");
      res.handle(Future.failedFuture("No X-Okapi-Url-to header"));
      return;
    }
    final String endPointUrl = okapiUrl + "/" + uriPath;
    List<Future> futures = new LinkedList<>();
    try {
      List<URL> urls = getURLsFromClassPathDir(filePath);
      if (urls.isEmpty()) {
        log.info("loadData getURLsFromClassPathDir returns empty list");
      }
      for (URL url : urls) {
        InputStream stream = url.openStream();
        if (stream == null) {
          log.warn("Null stream filename " + url.toString());
          res.handle(Future.failedFuture("Null stream filename " + url.toString()));
          return;
        }
        Future<Void> f = Future.future();
        futures.add(f);
        loadURL(headers, url, httpClient, useBasename, endPointUrl, f);
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
      return;

    } catch (IOException ex) {
      res.handle(Future.failedFuture("IOException for path " + filePath + " ex=" + ex.getLocalizedMessage()));
      return;
    }
  }

  public void performR(TenantAttributes ta, Map<String, String> headers, Iterator<LoadingEntry> it,
    HttpClient httpClient, int number, Handler<AsyncResult<Integer>> res) {
    if (!it.hasNext()) {
      res.handle(Future.succeededFuture(number));
    } else {
      LoadingEntry le = it.next();
      for (Parameter parameter : ta.getParameters()) {
        if (le.key.equals(parameter.getKey()) && "true".equals(parameter.getValue())) {
          loadData(headers, le.lead + File.separator + le.filePath, le.uriPath,
            le.useBasename, httpClient, x -> {
              if (x.failed()) {
                res.handle(Future.failedFuture(x.cause()));
              } else {
                performR(ta, headers, it, httpClient, number + x.result(), res);
              }
            });
          return;
        }
      }
      performR(ta, headers, it, httpClient, number, res);
    }
  }

  public void perform(TenantAttributes ta, Map<String, String> headers,
    Vertx vertx, Handler<AsyncResult<Integer>> handler) {
    Iterator<LoadingEntry> it = loadingEntries.iterator();
    HttpClient httpClient = vertx.createHttpClient();
    performR(ta, headers, it, httpClient, 0, res -> {
      handler.handle(res);
      httpClient.close();
    });
  }

  public void addJsonIdContent(String key, String lead, String filePath,
    String uriPath) {
    loadingEntries.add(new LoadingEntry(key, lead, filePath, uriPath));
  }

  public void addJsonIdBasename(String key, String lead, String filePath, String uriPath) {
    LoadingEntry le = new LoadingEntry(key, lead, filePath, uriPath);
    le.useBasename = true;
    loadingEntries.add(le);
  }
}
