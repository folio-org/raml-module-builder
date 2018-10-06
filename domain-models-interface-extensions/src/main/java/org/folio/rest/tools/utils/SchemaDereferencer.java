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

public class SchemaDereferencer {

  static final Logger log = LoggerFactory.getLogger(SchemaDereferencer.class);

  protected JsonObject dereferencedSchema2(Path path, Path tPath) throws IOException {
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

  private void fixupRef(Path path, JsonObject jsonObject)
      throws IOException {
    for (Entry<String,Object> entry : jsonObject) {
      Object value = entry.getValue();
      if (value instanceof JsonObject) {
        fixupRef(path, (JsonObject) value);
      }
    }
    String file = jsonObject.getString("$ref");
    if (file != null && !file.startsWith("file:")) {
      Path nPath = path.resolveSibling(file);
      jsonObject.put("$ref", "file:" + nPath.toAbsolutePath().normalize());
    }
  }
}
