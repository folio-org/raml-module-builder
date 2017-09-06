package org.folio.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import io.vertx.core.json.JsonObject;

/**
 * Dereference JSON schemas of RAML files by merging the file content
 * of any {@code ("$ref": <filename>)}.
 *
 * An instance of SchemaDereferencer forever caches the content of any
 * file it has already used.
 */
public class SchemaDereferencer {
  private String basePath;
  private Map<File,JsonObject> dereferenced = new HashMap<>();

  /**
   * Set the base path all $ref filenames are based on.
   * @param basePath  file system path
   */
  public SchemaDereferencer(final String basePath) {
    this.basePath = basePath;
  }

  /**
   * Dereference JSON schema files.
   *
   * Take the file pairs (inputFile, outputFile) from the command line arguments and
   * dereference the inputFile and write the result to outputFile.
   *
   * Example maven usage:
   * <blockquote><pre>{@code
   * <plugin>
   *   <groupId>org.codehaus.mojo</groupId>
   *   <artifactId>exec-maven-plugin</artifactId>
   *   <version>1.6.0</version>
   *   <executions>
   *     <execution>
   *       <id>dereference-schemas</id>
   *       <phase>process-classes</phase>
   *       <goals>
   *         <goal>java</goal>
   *       </goals>
   *       <configuration>
   *         <mainClass>org.folio.util.SchemaDereferencer</mainClass>
   *         <arguments>
   *           <argument>${project.basedir}/ramls/schemas/message.schema</argument>
   *           <argument>${project.build.directory}/classes/schemas/message.schema</argument>
   *           <argument>${project.basedir}/ramls/schemas/messages.schema</argument>
   *           <argument>${project.build.directory}/classes/schemas/messages.schema</argument>
   *         </arguments>
   *       </configuration>
   *     </execution>
   *   </executions>
   * </plugin>
   * }</pre></blockquote>
   *
   * These can then be read using org.folio.util.ResourceUtil.asString("schemas/message.schema").
   *
   * @param args  file pairs (inputFile, outputFile)
   * @throws IOException  when reading or writing a file fails.
   */
  public static void main(String[] args) throws IOException {
    if (args.length == 0) {
      System.err.println("arguments expected: [inputFile outputFile] ...");
      return;
    }

    if (args.length % 2 == 1) {
      System.err.println("number of arguments must be even (inputFile + outputFile), but it is odd: "
          + args.length);
      return;
    }

    for (int i=0; i<args.length; i+=2) {
      File inputFile = new File(args[i]);
      File outputFile = new File(args[i+1]);
      Path outputDir = Paths.get(outputFile.getAbsolutePath()).getParent();

      String basePath = inputFile.getParent();
      if (basePath == null) {
        basePath = ".";
      }
      SchemaDereferencer schemaDereferencer = new SchemaDereferencer(basePath);
      String json = schemaDereferencer.dereferencedSchema(inputFile.getName()).encodePrettily();
      Files.createDirectories(outputDir);
      try (PrintWriter printWriter = new PrintWriter(outputFile)) {
        printWriter.println(json);
      }
    }
  }

  /**
   * Return a schema with all $ref dereferenced.
   *
   * @param file  path and name of the schema file relative to basePath
   * @return dereferenced schema
   * @throws IOException  when any $ref file cannot be read
   * @throws IllegalStateException  when the $ref chain has a loop
   */
  public JsonObject dereferencedSchema(final String file) throws IOException {
    return dereferencedSchema(file, new ArrayDeque<>());
  }

  /**
   * Return a schema with all $ref dereferenced.
   *
   * @param file  path and name of the schema file relative to basePath
   * @param dereferenceStack  stack of the files chain for file
   * @return dereferenced RAML
   * @throws IOException  when any $ref file cannot be read
   * @throws IllegalStateException  when the $ref chain has a loop
   */
  protected JsonObject dereferencedSchema(final String file, Deque<String> dereferenceStack) throws IOException {
    String pathString = basePath + File.separator + file;
    File path;
    try {
      path = new File(pathString).getCanonicalFile();
    } catch (IOException e) {
      // getCanonicalFile() doesn't mention the pathString in the IOException
      throw new IOException(e.getMessage() + ": " + pathString, e);
    }

    if (dereferenced.containsKey(path)) {
      return dereferenced.get(path);
    }

    boolean loop = dereferenceStack.contains(file);
    dereferenceStack.push(file);

    if (loop) {
      Iterable<String> reverseStack = () -> dereferenceStack.descendingIterator();
      throw new IllegalStateException("$ref chain has a loop: " + String.join(", ", reverseStack));
    }

    String schemaString;
    try (InputStream reader = new FileInputStream(path)) {
      schemaString = IOUtil.toUTF8String(reader);
    }
    JsonObject schema = new JsonObject(schemaString);
    dereference(schema, dereferenceStack);

    dereferenceStack.pop();
    dereferenced.put(path,  schema);

    return schema;
  }

  /**
   * Merge the file content of any {@code ("$ref": <filename>)} into jsonObject.
   *
   * @param jsonObject  where to replace $ref
   * @param dereferenceStack  dereferenceStack to check for loops
   * @throws IllegalArgumentException  on $ref loop
   * @throws IOException  when any $ref file cannot be read
   * @throws ClassCastException  when $ref is not a String
   */
  private void dereference(JsonObject jsonObject, Deque<String> dereferenceStack) throws IOException {
    for (Entry<String,Object> entry : jsonObject) {
      Object value = entry.getValue();
      if (value instanceof JsonObject) {
        dereference((JsonObject) value, dereferenceStack);
      }
    }
    // merge $ref after the for loop to avoid processing merged values
    String file = jsonObject.getString("$ref");
    if (file == null) {
      return;
    }
    JsonObject schema = dereferencedSchema(file, dereferenceStack);
    jsonObject.mergeIn(schema);
  }
}
