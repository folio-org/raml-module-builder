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
import java.net.URISyntaxException;

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
      try {
        URI u = new URI("file", nPath.toAbsolutePath().normalize().toString(), null);
        jsonObject.put("$ref", u.toString());
      } catch (URISyntaxException ex) {
        throw new IOException(ex.getLocalizedMessage());
      }
    }
  }
}
