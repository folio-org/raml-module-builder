package org.folio.rest.tools.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map.Entry;

import org.folio.util.IoUtil;

import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import java.net.URI;

public class SchemaDereferencer {

  static final Logger log = LoggerFactory.getLogger(SchemaDereferencer.class);

  protected JsonObject dereferencedSchema(Path path, Path tPath) throws IOException {
    String content;
    try (InputStream reader = new FileInputStream(path.toFile())) {
      content = IoUtil.toStringUtf8(reader);
    }
    JsonObject schema = null;
    try {
      schema = new JsonObject(content);
    } catch (DecodeException e) {
      throw new DecodeException(e.getLocalizedMessage());
    }
    fixupRef(tPath, schema);
    return schema;
  }

  private boolean hasUriScheme(String uri) {
    for (int i = 0; i < uri.length(); i++) {
      char ch = uri.charAt(i);
      if (ch == ':') {
        return i >= 2;  // note drive letters
      }
      if (ch == '/' || ch == '\\') {
        break;
      }
    }
    return false;
  }

  /**
   * Replace each $ref value containing a relative path by
   * a file URI with an absolute path.
   *
   * <p>Examples for <code>"$ref": "dir/a.json"</code>:
   *
   * <ul>
   * <li>If the base path is <code>"/home/peter"</code> the ref
   * becomes <code>"$ref": "file:/home/peter/dir/a.json"</code>.
   * <li>If the base path is <code>"C:\Users\peter"</code> the ref
   * becomes <code>"$ref": "file:///C:\Users\peter\dir\a.json"</code>.
   * </ul>
   *
   * <p>The absolute path is needed for generating the code from raml files
   * because raml-java-parser fails on relative JSON refs. See
   * <a href="https://issues.folio.org/browse/RMB-265">RMB-265</a> and the
   * <a href="https://github.com/raml-org/raml-java-parser/issues/362">bug report</a>.
   *
   * @param path  base path
   * @param jsonObject  where to search and replace recursively
   */
  private void fixupRef(Path path, JsonObject jsonObject)
      throws IOException {
    for (Entry<String,Object> entry : jsonObject) {
      Object value = entry.getValue();
      if (value instanceof JsonObject) {
        fixupRef(path, (JsonObject) value);
      }
    }
    String file = jsonObject.getString("$ref");
    if (file != null && !hasUriScheme(file)) {
      Path nPath = path.resolveSibling(file);
      //fix the problem of uri.getpath()==null in windows environment
      jsonObject.put("$ref", nPath.toUri().toString().replaceAll("/\\./", "/"));
    }
  }
}
