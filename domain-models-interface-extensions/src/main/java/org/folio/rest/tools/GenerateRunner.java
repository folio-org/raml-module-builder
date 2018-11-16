package org.folio.rest.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.io.IOCase;
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

  public static final String SOURCES_DEFAULT = "ramls";
  public static final String RAML_LIST = "raml.list";
  public static final String JSON_SCHEMA_LIST = "json-schema.list";

  static {
    System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME, "io.vertx.core.logging.Log4j2LogDelegateFactory");
  }

  static final Logger log = LoggerFactory.getLogger(GenerateRunner.class);

  private static final String MODEL_PACKAGE_DEFAULT = "org.folio.rest.jaxrs.model";
  private static final String RESOURCE_DEFAULT = "target/classes";

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
    outputDirectoryWithPackage = outputDirectory + RTFConsts.INTERFACE_PACKAGE.replace('.', '/');
    configuration = new Configuration();
    configuration.setModelPackage(MODEL_PACKAGE_DEFAULT);
    configuration.setResourcePackage(RTFConsts.INTERFACE_PACKAGE);
    configuration.setSupportPackage(RTFConsts.INTERFACE_PACKAGE +".support");
    configuration.setOutputDirectory(new File(this.outputDirectory));
    configuration.setJsonMapper(AnnotationStyle.valueOf(("jackson2").toUpperCase()));
    configuration.setTypeConfiguration(new String[]{"core.one"});
    Map<String, String> config = new HashMap<>();
    config.put("customAnnotator", "org.folio.rest.tools.plugins.CustomTypeAnnotator");
    config.put("isIncludeJsr303Annotations", "true");
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

    createLookupList(output, RAML_LIST, ".raml");
    createLookupList(output, JSON_SCHEMA_LIST, ".json", ".schema");
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
        numMatches++;
      }
      else{
        log.info(ramls[j] + " has a .raml suffix but does not start with #%RAML");
      }
    }
    log.info("processed: " + numMatches + " raml files");
  }

  public static void createLookupList(File directory, String name, String...suffixes) throws IOException {
    File listFile = new File(directory.getAbsolutePath() + File.separator + name);
    FileOutputStream fos = new FileOutputStream(listFile);
    try(BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos))) {
      for (File file: directory.listFiles((FileFilter) new SuffixFileFilter(suffixes, IOCase.INSENSITIVE))) {
        log.info("lookup entry: " + file.getName());
        bw.write(file.getName());
        bw.newLine();
      }
    }
    log.info("lookup list file created: " + listFile.getAbsolutePath());
  }

  private static File rebase(String path) {
    File input = new File(path);
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

}
