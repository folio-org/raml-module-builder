package org.folio.rest.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.folio.rest.tools.plugins.CustomTypeAnnotator;
import org.folio.rest.tools.utils.RamlDirCopier;
import org.jsonschema2pojo.AnnotationStyle;
import org.raml.jaxrs.generator.Configuration;
import org.raml.jaxrs.generator.RamlScanner;

import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

/**
 * Read RAML files and generate .java files from them.
 *
 * Copy RAML files and schema files to target.
 */
public class GenerateRunner {

  static final Logger log = LoggerFactory.getLogger(GenerateRunner.class);

  private static final String PACKAGE_DEFAULT = "org.folio.rest.jaxrs.resources";
  private static final String MODEL_PACKAGE_DEFAULT = "org.folio.rest.jaxrs.model";
  private static final String SOURCES_DEFAULT = "/ramls/";
  private static final String RESOURCE_DEFAULT = "/target/classes";

  private String outputDirectory = null;
  private String outputDirectoryWithPackage = null;
  private String modelDirectory = null;
  private Configuration configuration = null;

  private boolean usingDefaultCustomField = true;

  private Set<String> injectedAnnotations = new HashSet<>();

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
    outputDirectoryWithPackage = outputDirectory + PACKAGE_DEFAULT.replace('.', '/');
    modelDirectory = outputDirectory + MODEL_PACKAGE_DEFAULT.replace('.', '/');

    configuration = new Configuration();
    configuration.setModelPackage(MODEL_PACKAGE_DEFAULT);
    configuration.setResourcePackage(PACKAGE_DEFAULT);
    configuration.setSupportPackage(PACKAGE_DEFAULT);
    configuration.setOutputDirectory(new File(this.outputDirectory));
    configuration.setJsonMapper(AnnotationStyle.valueOf(("jackson2").toUpperCase()));
    configuration.setTypeConfiguration(new String[]{"core.one"});
    Map<String, String> config = new HashMap<>();
    config.put("customAnnotator", "org.folio.rest.tools.plugins.CustomTypeAnnotator");
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
  public static void main(String [] args) throws Exception {

    String root = System.getProperties().getProperty("project.basedir");
    if (root == null) {
      root = new File(".").getAbsolutePath();
    }
    String outputDirectory = root + ClientGenerator.PATH_TO_GENERATE_TO;

    GenerateRunner generateRunner = new GenerateRunner(outputDirectory);
    //generateRunner.cleanDirectories();
    CustomTypeAnnotator.setCustomFields(System.getProperties().getProperty("jsonschema.customfield"));

    String parentRoot = System.getProperties().getProperty("maven.multiModuleProjectDirectory");
    copyRamlDirToTarget(root, parentRoot);

    String ramlsDir = System.getProperty("raml_files", root + SOURCES_DEFAULT);
    String [] paths = ramlsDir.split(","); //if multiple paths are indicated with a , delimiter
    for (String inputDirectory : paths) {
      generateRunner.generate(inputDirectory);
    }

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
    String clientDir = outputDirectory + RTFConsts.CLIENT_GEN_PACKAGE.replace('.', '/');
    ClientGenerator.makeCleanDir(clientDir);
  }


  /**
   * Copy the files from the /raml/ directory to the /target/raml/ directory
   * and dereference all schemas (*.schema, *.json) that contain a $ref reference.
   * It uses root if it exists, parentRoot otherwise. It does nothing if neither exist.
   *
   * @param root  base directory where the /raml/ and the /target/ directory are.
   * @param parentRoot  base directory where the /raml/ and the /target/ directory are.
   * @throws IOException on file copy error
   */
  public static void copyRamlDirToTarget(String root, String parentRoot) throws IOException {
    File input = new File(root + SOURCES_DEFAULT);
    if (! input.exists()) {
      if (parentRoot == null) {
        return;
      }
      input = new File(parentRoot + SOURCES_DEFAULT);
      if (! input.exists()) {
        return;
      }
    }
    File output = new File(root + RESOURCE_DEFAULT + File.separator + SOURCES_DEFAULT);
    log.info("copying ramls from source directory at: " + input);
    log.info("copying ramls to target directory at: " + output);
    RamlDirCopier.copy(input.toPath(), output.toPath());
  }

  /**
   * Generate the .java files from the .raml files.
   * @param inputDirectory  where to search for .raml files
   * @throws Exception  on read, write or validate error
   */
  public void generate(String inputDirectory) throws Exception {
    log.info( "Input directory " + inputDirectory);
    File inputDir = new File(inputDirectory);
    if (! inputDir.isDirectory()) {
      throw new IOException("Input path is not a valid directory: " + inputDirectory);
    }
    configuration.setOutputDirectory(new File(outputDirectory));
    //configuration.setSourceDirectory(inputDir);
    int numMatches = 0;

    File []ramls = new File(inputDirectory).listFiles( (dir, name) -> name.endsWith(".raml") );
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
        //GENERATOR.run(new FileReader(ramls[j]), configuration, ramls[j].getAbsolutePath());
        numMatches++;
      }
      else{
        log.info(ramls[j] + " has a .raml suffix but does not start with #%RAML");
      }
    }
/*    for (int j = 0; j < schemaCustomFields.length; j++) {
      JsonObject jo = new JsonObject(schemaCustomFields[j]);
      String fieldName = jo.getString("fieldname");
      Object fieldValue = jo.getValue("fieldvalue");
      String annotation = jo.getString("annotation");
      log.info("processing unreferenced schemas. looking for " + fieldName + " with value " + fieldValue);
      processRemainingSchemas(globalUnprocessedSchemas, processedPojos, fieldName, fieldValue, annotation);
    }*/
    log.info("processed: " + numMatches + " raml files");
  }

}
