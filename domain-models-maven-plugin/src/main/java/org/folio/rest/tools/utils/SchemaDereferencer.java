package org.folio.rest.tools.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map.Entry;
import java.util.logging.Logger;
import org.folio.util.IoUtil;

import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;

public class SchemaDereferencer {

  static final Logger log = Logger.getLogger(SchemaDereferencer.class.getName());

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

  private static boolean hasUriScheme(String uri) {
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
   * <li>If the base path file is <code>"/home/peter.json"</code> the ref
   * becomes <code>"$ref": "file:///home/dir/a.json"</code>.
   * <li>If the base path file is <code>"C:\Users\peter.json"</code> the ref
   * becomes <code>"$ref": "file:///C:/Users/dir/a.json"</code>.
   * </ul>
   *
   * <p>The absolute path is needed for generating the code from raml files
   * because raml-java-parser fails on relative JSON refs. See
   * <a href="https://issues.folio.org/browse/RMB-265">RMB-265</a> and the
   * <a href="https://github.com/raml-org/raml-java-parser/issues/362">bug report</a>.
   *
   * @param basePathFile  path of a file in the base path, the base path is used to
   *                       resolve the relative path
   * @param jsonObject  where to search and replace recursively
   */
  static void fixupRef(Path basePathFile, JsonObject jsonObject)
      throws IOException {
    for (Entry<String,Object> entry : jsonObject) {
      Object value = entry.getValue();
      if (value instanceof JsonObject) {
        fixupRef(basePathFile, (JsonObject) value);
      }
    }
    Object value = jsonObject.getValue("$ref");
    if (value == null) {
      return;
    }
    if (! (value instanceof String)) {
      throw new ClassCastException("\"$ref\" value must of type String, but it is of type "
          + value.getClass().getName() + ": " + value);
    }
    String file = (String) value;
    if (hasUriScheme(file)) {
      return;
    }
    jsonObject.put("$ref", toFileUri(basePathFile, file));
  }

  /**
   * Return <code>file</code> as an URI with absolute path with sibling base as input.
   *
   * <p>Examples:
   *
   * <ul>
   * <li>Linux: <code>toFileUri(new File("/home/peter.json").toPath(), "dir/a.json") = "file:///home/dir/a.json"</code>
   * <li>Windows: <code>toFileUri(new File("C:\Users\peter.json").toPath(), "dir/a.json") = "file:///C:/Users/dir/a.json"</code>
   * </ul>
   *
   * @param basePathFile existing path for component that is used as base sibling for path <code>file</code>.
   * @param file the relative path
   * @return the absolute path as a URI
   */
  static String toFileUri(Path basePathFile, String file) {
    Path absolutePath = basePathFile.resolveSibling(file).normalize();
    return absolutePath.toUri().toString();
  }
}
