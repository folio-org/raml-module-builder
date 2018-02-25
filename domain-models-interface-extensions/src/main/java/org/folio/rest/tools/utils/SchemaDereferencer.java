package org.folio.rest.tools.utils;

import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.folio.util.IoUtil;

/**
 * Dereference JSON schemas of RAML files by replacing {@code ("$ref": <filename>)}
 * with the JSON content of that file.
 *
 * An instance of SchemaDereferencer forever caches the content of any
 * file it has already used.
 */
public class SchemaDereferencer {
  /** cache */
  private Map<Path,JsonObject> dereferenced = new HashMap<>();

  /**
   * Return a schema with all $ref dereferenced.
   *
   * @param path  path of the schema file; it may be relative to basePath
   * @return dereferenced schema
   * @throws IOException  when any $ref file cannot be read
   * @throws IllegalStateException  when the $ref chain has a loop
   */
  protected JsonObject dereferencedSchema(Path path) throws IOException {
    return dereferencedSchema(path, new ArrayDeque<>());
  }

  /**
   * Create a stream from the iterable.
   * @param iterable  what to iterate
   * @return the stream
   */
  private static <T> Stream<T> stream(Iterable<T> iterable) {
    return StreamSupport.stream(iterable.spliterator(), false);
  }

  /**
   * The paths in descending order, comma separated.
   */
  private String allPaths(Deque<Path> dereferenceStack) {
    Iterable<Path> reverseStack = dereferenceStack::descendingIterator;
    return stream(reverseStack).map(Path::toString).collect(Collectors.joining(", "));
  }

  /**
   * Return a schema with all $ref dereferenced.
   *
   * @param inputPath  path of the schema file, may be relative to the current directory
   * @param dereferenceStack  stack of the files chain for file
   * @return dereferenced RAML
   * @throws IOException  when any $ref file cannot be read
   * @throws IllegalStateException  when the $ref chain has a loop
   * @throws DecodeException  when the $ref file is not a JSON
   */
  protected JsonObject dereferencedSchema(Path inputPath, Deque<Path> dereferenceStack)
      throws IOException {

    Path path = inputPath.normalize().toAbsolutePath();

    if (dereferenced.containsKey(path)) {
      return dereferenced.get(path);
    }

    boolean loop = dereferenceStack.contains(path);
    dereferenceStack.push(path);

    if (loop) {
      throw new IllegalStateException("$ref chain has a loop: " + allPaths(dereferenceStack));
    }

    String schemaString;
    try (InputStream reader = new FileInputStream(path.toFile())) {
      schemaString = IoUtil.toStringUtf8(reader);
    }

    JsonObject schema;
    if (schemaString.indexOf('{') == -1) {
      // schemaString contains the filename to open
      Path newInputPath = inputPath.resolveSibling(Paths.get(schemaString)).normalize();
      schema = dereferencedSchema(newInputPath, dereferenceStack);
    } else {
      try {
        schema = new JsonObject(schemaString);
      } catch (DecodeException e) {
        throw new DecodeException(allPaths(dereferenceStack), e);
      }
      dereference(schema, inputPath, dereferenceStack);
    }

    dereferenceStack.pop();
    dereferenced.put(path, schema);

    return schema;
  }

  /**
   * Merge the file content of any {@code ("$ref": <filename>)} into jsonObject.
   *
   * @param jsonObject  where to replace $ref
   * @param jsonFile  the path of jsonObject
   * @param dereferenceStack  dereferenceStack to check for loops
   * @throws IllegalArgumentException  on $ref loop
   * @throws IOException  when any $ref file cannot be read
   * @throws ClassCastException  when $ref is not a String
   */
  private void dereference(JsonObject jsonObject, Path jsonPath, Deque<Path> dereferenceStack)
      throws IOException {
    for (Entry<String,Object> entry : jsonObject) {
      Object value = entry.getValue();
      if (value instanceof JsonObject) {
        dereference((JsonObject) value, jsonPath, dereferenceStack);
      }
    }
    // merge $ref after the for loop to avoid processing merged values
    String file = jsonObject.getString("$ref");
    if (file == null) {
      return;
    }
    jsonObject.remove("$ref");
    Path refPath = jsonPath.resolveSibling(Paths.get(file)).normalize();
    JsonObject schema = dereferencedSchema(refPath, dereferenceStack);
    jsonObject.mergeIn(schema);
  }
}
