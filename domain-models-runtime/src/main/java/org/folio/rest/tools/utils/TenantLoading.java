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
import java.io.FileInputStream;
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

  private TenantLoading() {
    throw new UnsupportedOperationException("Cannot instantiate");
  }

  private static List<InputStream> getStreamsfromClassPathDir(String directoryName)
    throws URISyntaxException, IOException {

    List<InputStream> streams = new LinkedList<>();
    URL url = Thread.currentThread().getContextClassLoader().getResource(directoryName);
    if (url != null) {
      if (url.getProtocol().equals("file")) {
        File file = Paths.get(url.toURI()).toFile();
        if (file != null) {
          File[] files = file.listFiles();
          if (files != null) {
            for (File filename : files) {
              streams.add(new FileInputStream(filename));
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
              streams.add(Thread.currentThread().getContextClassLoader().getResourceAsStream(name));
            }
          }
        }
      }
    }
    return streams;
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

  private static void loadData(Map<String, String> headers, String lead, String endPoint,
    HttpClient httpClient, Handler<AsyncResult<Integer>> res) {

    if (endPoint.isEmpty()) {
      res.handle(Future.succeededFuture(0));
      return;
    }
    final String[] comp = endPoint.split("\\s+");
    final String filePath = lead + "/" + comp[0];
    String uriPath = comp[0];
    if (comp.length >= 2) {
      uriPath = comp[1];
    }
    log.info("loadData uriPath=" + uriPath + " filePath=" + filePath);
    String okapiUrl = headers.get("X-Okapi-Url-to");
    if (okapiUrl == null) {
      log.warn("loadData No X-Okapi-Url-to header");
      res.handle(Future.failedFuture("No X-Okapi-Url-to header"));
      return;
    }
    List<String> jsonList = new LinkedList<>();
    try {
      List<InputStream> streams = getStreamsfromClassPathDir(filePath);
      for (InputStream stream : streams) {
        jsonList.add(IOUtils.toString(stream, "UTF-8"));
      }
    } catch (URISyntaxException ex) {
      res.handle(Future.failedFuture("URISyntaxException for path " + filePath + " ex=" + ex.getLocalizedMessage()));
      return;

    } catch (IOException ex) {
      res.handle(Future.failedFuture("IOException for path " + filePath + " ex=" + ex.getLocalizedMessage()));
      return;
    }
    Integer sz = jsonList.size();
    final String endPointUrl = okapiUrl + "/" + uriPath;
    List<Future> futures = new LinkedList<>();
    for (String json : jsonList) {
      Future f = Future.future();
      futures.add(f);
      JsonObject jsonObject = new JsonObject(json);
      String id = jsonObject.getString("id");
      if (id == null) {
        res.handle(Future.failedFuture("Missing id for " + json));
        return;
      }
      HttpClientRequest reqPut = httpClient.putAbs(endPointUrl + "/" + id, resPut -> {
        if (resPut.statusCode() == 404) {
          HttpClientRequest reqPost = httpClient.postAbs(endPointUrl, resPost -> {
            if (resPost.statusCode() == 201) {
              f.handle(Future.succeededFuture());
            } else {
              f.handle(Future.failedFuture("POST " + endPointUrl + " returned status " + resPost.statusCode()));
            }
          });
          reqPost.exceptionHandler(x -> {
            f.handle(Future.failedFuture("POST " + endPointUrl + " failed"));
          });
          endWithXHeaders(reqPost, headers, json);
        } else if (resPut.statusCode() == 200) {
          f.handle(Future.succeededFuture());
        } else {
          f.handle(Future.failedFuture("PUT " + endPointUrl + "/" + id + " returned status " + resPut.statusCode()));
        }
      });
      reqPut.exceptionHandler(x -> {
        f.handle(Future.failedFuture("PUT " + endPointUrl + "/" + id + " failed"));
      });
      endWithXHeaders(reqPut, headers, json);
    }
    CompositeFuture.all(futures).setHandler(x -> {
      if (x.failed()) {
        res.handle(Future.failedFuture(x.cause().getLocalizedMessage()));
      } else {
        res.handle(Future.succeededFuture(sz));
      }
    });
  }

  private static void load(Map<String, String> headers, String lead, Iterator<String> it,
    HttpClient httpClient, int number, Handler<AsyncResult<Integer>> res) {

    if (!it.hasNext()) {
      res.handle(Future.succeededFuture(number));
    } else {
      String endPoint = it.next();
      loadData(headers, lead, endPoint, httpClient, x -> {
        if (x.failed()) {
          res.handle(Future.failedFuture(x.cause()));
        } else {
          load(headers, lead, it, httpClient, number + x.result(), res);
        }
      });
    }
  }

  private static void load(Map<String, String> headers, String lead, List<String> paths,
    Vertx vertx, Handler<AsyncResult<Integer>> handler) {

    HttpClient httpClient = vertx.createHttpClient();
    load(headers, lead, paths.iterator(), httpClient, 0, res -> {
      httpClient.close();
      handler.handle(res);
    });
  }

  public static void load(TenantAttributes ta, Map<String, String> headers,
    String key, String lead, List<String> paths, Vertx vertx,
    Handler<AsyncResult<Integer>> handler) {

    for (Parameter parameter : ta.getParameters()) {
      if (key.equals(parameter.getKey()) && "true".equals(parameter.getValue())) {
        load(headers, lead, paths, vertx, handler);
        return;
      }
    }
    handler.handle(Future.succeededFuture(0));
  }
}
