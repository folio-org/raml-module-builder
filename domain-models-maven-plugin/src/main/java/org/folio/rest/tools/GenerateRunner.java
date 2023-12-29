package org.folio.rest.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.folio.rest.tools.plugins.CustomTypeAnnotator;
import org.folio.rest.tools.utils.RamlDirCopier;
import org.jsonschema2pojo.AnnotationStyle;
import org.raml.jaxrs.generator.Configuration;
import org.raml.jaxrs.generator.RamlScanner;

/**
 * Read RAML files and generate .java files from them.
 *
 * Copy RAML files and schema files to target.
 */
public class GenerateRunner {

  public static final String SOURCES_DEFAULT = "ramls";
  public static final String RAML_LIST = "raml.list";
  public static final String JSON_SCHEMA_LIST = "json-schema.list";
  public static final String DEFAULT_SCHEMA_DIRECTORY = "";
  static final String RESOURCE_DEFAULT = "target/classes";

  static final Logger log = Logger.getLogger(GenerateRunner.class.getName());

  private static final String MODEL_PACKAGE_DEFAULT = "org.folio.rest.jaxrs.model";
  private static final String SCHEMA_CONFIG_PROPERTY_PREFIX = "jsonschema2pojo.config.";

  private String outputDirectory = null;
  private String outputDirectoryWithPackage = null;
  private Configuration configuration = null;

  /**
   * Create a GenerateRunner for a specific target directory.
   * <p>
   * The output directory of the .java client is
   * <code>target/generated-sources/raml-jaxrs/org/folio/rest/client</code>,
   * the output directory of the .java pojos is
   * <code>target/generated-sources/raml-jaxrs/org/folio/rest/jaxrs/model</code>,
   * the output directory of the RAML and dereferenced schema files is
   * <code>target/classes</code>; they are relative to the parameter
   * <code>outputDirectory</code>.
   *
   * @param outputDirectory  where to write the files to
   */
  public GenerateRunner(String outputDirectory) {
    this.outputDirectory = outputDirectory;
    outputDirectoryWithPackage = outputDirectory + AnnotationGrabber.INTERFACE_PACKAGE.replace('.', '/');
    configuration = new Configuration();
    configuration.setModelPackage(MODEL_PACKAGE_DEFAULT);
    configuration.setResourcePackage(AnnotationGrabber.INTERFACE_PACKAGE);
    configuration.setSupportPackage(AnnotationGrabber.INTERFACE_PACKAGE +".support");
    configuration.setOutputDirectory(new File(this.outputDirectory));
    configuration.setJsonMapper(AnnotationStyle.valueOf(("jackson2").toUpperCase()));
    configuration.setTypeConfiguration(new String[]{"core.one"});
    Map<String, String> config = new HashMap<>();
    config.put("customAnnotator", "org.folio.rest.tools.plugins.CustomTypeAnnotator");
    config.put("isIncludeJsr303Annotations", "true");
    copyConfigProperties(System.getProperties(), config);
    configuration.setJsonMapperConfiguration(config);

  }

  /**
   * Reads RAML and schema files and writes the generated .java files.
   * <p>
   * Copy the /ramls/ directory to /target/classes/ramls/ and dereference schema files.
   * <p>
   * The input directories of the RAML files are listed
   * in system property <code>raml_files</code> and are comma separated.
   * Default is <code>project.basedir</code>/ramls/.
   * <p>
   * The output directories are relative to the directory
   * specified by the system property <code>project.basedir</code>, see
   * {@link #GenerateRunner(String)}. Default is the current directory.
   * <p>
   * Any existing content in the output directories is removed.
   *
   * @param args  are ignored
   * @throws Exception  on file read or file write error
   */
  public static void main(String[] args) throws Exception {

    String root = System.getProperties().getProperty("project.basedir");
    if (root == null) {
      root = new File(".").getAbsolutePath();
    }
    String outputDirectory = root + ClientGenerator.PATH_TO_GENERATE_TO;

    GenerateRunner generateRunner = new GenerateRunner(outputDirectory);
    //generateRunner.cleanDirectories();
    CustomTypeAnnotator.setCustomFields(System.getProperties().getProperty("jsonschema.customfield"));

    String [] ramlFiles = System.getProperty("raml_files", SOURCES_DEFAULT).split(",");
    String [] schemaPaths = System.getProperty("schema_paths", DEFAULT_SCHEMA_DIRECTORY).split(",");

    File input = rebase(ramlFiles[0]);
    File output = new File(root + File.separator + RESOURCE_DEFAULT + File.separator + SOURCES_DEFAULT);

    log.info("copying ramls from source directory at: " + input);
    log.info("copying ramls to target directory at: " + output);
    RamlDirCopier.copy(input.toPath(), output.toPath());

    for (String d : ramlFiles) {
      File tmp  = new File(d);
      String a = tmp.getAbsolutePath();
      a = a.replace(input.getAbsolutePath(), output.getAbsolutePath());
      generateRunner.generate(a);
    }

    createLookupList(output, RAML_LIST, Collections.singletonList(".raml"));
    createLookupList(output, JSON_SCHEMA_LIST, Arrays.asList(".json", ".schema"), Arrays.asList(schemaPaths));
  }

  /**
   * Remove all files from the PACKAGE_DEFAULT and RTFConsts.CLIENT_GEN_PACKAGE
   * directories.
   * @throws IOException on file delete error
   */
  public void cleanDirectories() throws IOException {
    ClientGenerator.makeCleanDir(outputDirectoryWithPackage);

    //if we are generating interfaces, we need to remove any generated client code
    //as if the interfaces have changed in a way (pojos removed, etc...) that causes
    //the client generated code to cause compilation errors
    String clientDir = outputDirectory + AnnotationGrabber.CLIENT_GEN_PACKAGE.replace('.', '/');
    ClientGenerator.makeCleanDir(clientDir);
  }

  /**
   * Generate the .java files from the .raml files.
   * @param inputDirectory  where to search for .raml files
   * @throws IOException  on read, write or validate error
   */
  public void generate(String inputDirectory) throws IOException {
    System.out.println("GenerateRunner.generate Input directory " + inputDirectory);
    log.info( "Input directory " + inputDirectory);
    File inputDir = new File(inputDirectory);
    if (! inputDir.isDirectory()) {
      throw new IOException("Input path is not a valid directory: " + inputDirectory);
    }
    configuration.setOutputDirectory(new File(outputDirectory));
    int numMatches = 0;

    File []ramls = new File(inputDirectory).listFiles( (dir, name) -> name.endsWith(".raml") );
    Arrays.sort(ramls);
    RamlScanner scanner = new RamlScanner(configuration);
    for (int j = 0; j < ramls.length; j++) {
      String line;
      try (BufferedReader reader = new BufferedReader(new FileReader(ramls[j]))) {
        line = reader.readLine();
      }
      if(line.startsWith("#%RAML")) {
        log.info("processing " + ramls[j]);
        //generate java interfaces and pojos from raml
        scanner.handle(ramls[j]);
        log.info("processed " + ramls[j]);
        numMatches++;
      }
      else{
        log.info(ramls[j] + " has a .raml suffix but does not start with #%RAML");
      }
    }
    log.info("processed: " + numMatches + " raml files");
  }

  /**
   * Creates list of files in directory and writes it to file
   *
   * @param directory directory with files
   * @param name      name of new file with list
   * @param suffixes  list of file suffixes to be included in list
   */
  public static void createLookupList(File directory, String name, List<String> suffixes) throws IOException {
    createLookupList(directory, name, suffixes, Collections.singletonList(""));
  }

  /**
   * Creates list of files in directory and writes it to file.
   *
   * @param directory    base directory
   * @param name         name of new file with list
   * @param suffixes     list of file suffixes to be included in list
   * @param subdirectoryExpressions list of glob expressions that describe subdirectories that will be searched for schemas
   *                                (e.g. "schemas/**" will search directory schemas recursively, and "schemas" will only
   *                                search files that are stored immediately in "schemas" directory)
   */
  public static void createLookupList(File directory, String name, List<String> suffixes,
                                      List<String> subdirectoryExpressions) throws IOException {
    File listFile = new File(directory.getAbsolutePath() + File.separator + name);
    Path listPath = Paths.get(directory.getAbsolutePath(), name);

    List<PathMatcher> pathMatchers = subdirectoryExpressions.stream()
      .map(expression -> getPathMatcher(suffixes, expression))
      .collect(Collectors.toList());

    Path basePath = Paths.get(directory.getAbsolutePath());

    List<Path> paths;
    try(Stream<Path> pathStream = Files.walk(basePath)){
      paths = pathStream.map(basePath::relativize)
          .filter(path -> pathMatchers.stream()
                            .anyMatch(pathMatcher -> pathMatcher.matches(path)))
          .sorted()
          .collect(Collectors.toList());
    }

    try (BufferedWriter bw = Files.newBufferedWriter(listPath)) {
      for (Path path : paths) {
        String pathString = path.toString().replace(File.separator, "/");
        log.info("lookup entry: " + pathString);
        bw.write(pathString);
        bw.newLine();
      }
    }
    log.info("lookup list file created: " + listFile.getAbsolutePath());
  }

  private static PathMatcher getPathMatcher(List<String> suffixes, String relativePath) {
    String fileExpression = "*{" + String.join(",", suffixes) + "}";
    if(!relativePath.isEmpty() && !relativePath.endsWith("/") && !relativePath.endsWith("**")){
      relativePath = relativePath + "/";
    }
    return FileSystems.getDefault()
      .getPathMatcher("glob:" + relativePath + fileExpression);
  }

  static File rebase(String path) {
    return rebase(new File(path));
  }

  static File rebase(File input) {
    File temp = input;
    while (true) {
      temp = temp.getParentFile();
      if (temp == null) {
        break;
      } else {
        if (temp.getName().equals(SOURCES_DEFAULT)) {
          input = temp;
        }
      }
    }
    return input;
  }

  /**
   * Copies properties that start with prefix SCHEMA_CONFIG_PROPERTY_PREFIX into specified map
   * @param properties Properties to copy
   * @param config target map
   */
  private void copyConfigProperties(Properties properties, Map<String, String> config) {
    properties.stringPropertyNames().stream()
      .filter(name -> name.startsWith(SCHEMA_CONFIG_PROPERTY_PREFIX))
      .forEach(name -> {
          String value = (String) properties.get(name);
          config.put(name.substring(SCHEMA_CONFIG_PROPERTY_PREFIX.length()), value);
        }
      );
  }
}
