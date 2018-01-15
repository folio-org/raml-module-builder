package org.folio.rest.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;
import org.folio.rest.tools.utils.RamlDirCopier;
import org.folio.rest.utils.GlobalSchemaPojoMapperCache;
import org.jsonschema2pojo.AnnotationStyle;
import org.raml.jaxrs.codegen.core.Configuration;
import org.raml.jaxrs.codegen.core.Configuration.JaxrsVersion;
import org.raml.jaxrs.codegen.core.GeneratorProxy;
import org.raml.jaxrs.codegen.core.ext.GeneratorExtension;
import org.raml.v2.api.model.v08.api.GlobalSchema;

import io.vertx.core.json.JsonObject;

/**
 * Read RAML files and generate .java files from them.
 *
 * Copy RAML files and schema files to target.
 */
public class GenerateRunner {

  private static final Logger log = Logger.getLogger(GenerateRunner.class);

  private static final GeneratorProxy GENERATOR = new GeneratorProxy();
  private static final String PACKAGE_DEFAULT = "org.folio.rest.jaxrs";
  private static final String MODEL_PACKAGE_DEFAULT = "org.folio.rest.jaxrs.model";
  private static final String SOURCES_DEFAULT = "/ramls/";
  private static final String RESOURCE_DEFAULT = "/target/classes";
  private static final String DEFAULT_CUSTOM_FIELD =
      "{\"fieldname\" : \"readonly\" , \"fieldvalue\": true , \"annotation\" : \"javax.validation.constraints.Null\"}";

  private String outputDirectory = null;
  private String outputDirectoryWithPackage = null;
  private String modelDirectory = null;
  private Configuration configuration = null;

  private boolean usingDefaultCustomField = true;
  private String [] schemaCustomFields = { DEFAULT_CUSTOM_FIELD };

  private Set<String> injectedAnnotations = new HashSet<>();

  /**
   * Create a GenerateRunner for a specific target directory.
   * <p>
   * The output directory of the .java client is
   * <code>src/main/java/org/folio/rest/client</code>,
   * the output directory of the .java pojos is
   * <code>src/main/java/org/folio/rest/jaxrs/model</code>,
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

    List<GeneratorExtension> extensions = new ArrayList<>();
    extensions.add(new Raml2Java());
    configuration = new Configuration();
    configuration.setJaxrsVersion(JaxrsVersion.JAXRS_2_0);
    configuration.setUseJsr303Annotations(true);
    configuration.setJsonMapper(AnnotationStyle.JACKSON2);
    configuration.setBasePackageName(PACKAGE_DEFAULT);
    configuration.setExtensions(extensions);
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
  public static void main(String [] args) throws Exception{

    String root = System.getProperties().getProperty("project.basedir");
    if (root == null) {
      root = new File(".").getAbsolutePath();
    }
    String outputDirectory = root + ClientGenerator.PATH_TO_GENERATE_TO;

    GenerateRunner generateRunner = new GenerateRunner(outputDirectory);
    generateRunner.cleanDirectories();
    generateRunner.setCustomFields(System.getProperties().getProperty("jsonschema.customfield"));

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
   * Set the JSON schemas of custom fields.
   * @param customFields the semicolon separated schemas, or null for default custom fields.
   */
  public void setCustomFields(String customFields) {
    if (customFields == null) {
      usingDefaultCustomField = true;
      schemaCustomFields = new String [] { DEFAULT_CUSTOM_FIELD };
    } else {
      usingDefaultCustomField = false;
      schemaCustomFields = customFields.split(";");
    }
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
    configuration.setSourceDirectory(inputDir);

    int numMatches = 0;

    File []ramls = new File(inputDirectory).listFiles( (dir, name) -> name.endsWith(".raml") );

    Set<String> processedPojos = new HashSet<>();
    List<List<GlobalSchema>> globalUnprocessedSchemas = new ArrayList<>();

    for (int j = 0; j < ramls.length; j++) {
      String line;
      try (BufferedReader reader = new BufferedReader(new FileReader(ramls[j]))) {
        line = reader.readLine();
      }
      if(line.startsWith("#%RAML")) {
        log.info("processing " + ramls[j]);
        //generate java interfaces and pojos from raml
        GENERATOR.run(new FileReader(ramls[j]), configuration, ramls[j].getAbsolutePath());
        numMatches++;

        //get list of schema to injectedFieldList
        //have a map of top level schemas (schemas used in the raml schema:, etc...) to pojos
        //scan fields in top level pojo list to get referenced pojos
        //the name of their schema will be the jsonproperty annotation name
        //check if for the top level pojo , the embedded objects need annotating

        List<GlobalSchema> schemaList = JsonSchemaPojoUtil.getSchemasFromRaml(ramls[j]);
        log.info("Schemas found in " + ramls[j]);
        schemaList.forEach( entry -> log.info("* " + entry.key()) );

        List<GlobalSchema> unprocessedSchemas = new ArrayList<>();
        unprocessedSchemas.addAll(schemaList);

        for (int k = 0; k < schemaCustomFields.length; k++) {
          String message = "";
          if(usingDefaultCustomField){
            message = "Using default: ";
          }
          log.info(message + " custom field " + schemaCustomFields[k]);
          JsonObject jo = new JsonObject(schemaCustomFields[k]);
          String fieldName = jo.getString("fieldname");
          Object fieldValue = jo.getValue("fieldvalue");
          String annotation = jo.getString("annotation");
          log.info("processing referenced schemas. looking for " + fieldName + " with value " + fieldValue);
          processReferencedSchemas(schemaList, unprocessedSchemas, processedPojos, fieldName, fieldValue, annotation, k);
        }
        globalUnprocessedSchemas.add(unprocessedSchemas);
      }
      else{
        log.info(ramls[j] + " has a .raml suffix but does not start with #%RAML");
      }
    }
    for (int j = 0; j < schemaCustomFields.length; j++) {
      JsonObject jo = new JsonObject(schemaCustomFields[j]);
      String fieldName = jo.getString("fieldname");
      Object fieldValue = jo.getValue("fieldvalue");
      String annotation = jo.getString("annotation");
      log.info("processing unreferenced schemas. looking for " + fieldName + " with value " + fieldValue);
      processRemainingSchemas(globalUnprocessedSchemas, processedPojos, fieldName, fieldValue, annotation);
    }
    log.info("processed: " + numMatches + " raml files");
  }

  private void processReferencedSchemas(List<GlobalSchema> schemaList,
      List<GlobalSchema> unprocessedSchemas, Set<String> processedPojos, String fieldName, Object fieldValue, String annotation, int k){
    //contains schemas referenced in the raml (not embedded ones) - to pojo mappings
    Set<Entry<Object, Object>> o = GlobalSchemaPojoMapperCache.getSchema2PojoMapper().entrySet();
    o.forEach( entry -> {
      String schema = FilenameUtils.getName(((URL)entry.getKey()).getPath()); // schema
      String pojo = (String)entry.getValue(); //pojo generated from that schema
      processedPojos.add(pojo);
      int schemasSize = schemaList.size();
      for (int l = 0; l < schemasSize; l++) {
        if(schemaList.get(l).key().equalsIgnoreCase(schema)){
          //get the fields in the schema that contain the field name we are looking for
          if(k==0){
            //remove from list of unprocessed schemas as we are now processing this schema
            unprocessedSchemas.remove(schemaList.get(l));
          }
          List<String> injectFieldList =
              JsonSchemaPojoUtil.getFieldsInSchemaWithType(new JsonObject(schemaList.get(l).value().value()), fieldName, fieldValue);
          String fullPathPojo = outputDirectory + pojo.replace('.', '/') + ".java";
          try {
            //check for dot annotation - split by '.'
            inject(fullPathPojo, annotation, injectFieldList);
          } catch (Exception e) {
            log.error(e.getMessage(), e);
          }
        }
      }
    });
  }

  private void inject(String rootPojo, String annotation, List<String> injectFieldList) throws Exception{
    int injectCount = injectFieldList.size();
    Set<String> annotateField4RootPojo = new HashSet<>();

    for (int i = 0; i < injectCount; i++) {
      //get a map between the json schema field (to compare to the injectFieldList) - to the
      //java type mapped to this json schema field
      Map<Object, Object> schemaFields2JavaTypes = JsonSchemaPojoUtil.jsonFields2Pojo(rootPojo);
      String field = injectFieldList.get(i);
      if(!field.contains(".")){
        annotateField4RootPojo.add(field);
      }
      else {
        //this is an annotation on an embedded object (json object embedded within a schema)
        String [] hierarchyOfObjects = field.split("\\.");
        //loop over the hierarchy. the last entry will be the field name
        //so we need to loop over the generated objects and then annotate the field name
        for (int j = 0; j < hierarchyOfObjects.length-1; j++) {
          //get the java type of the field
          Object javaType = schemaFields2JavaTypes.get(hierarchyOfObjects[j]);
          log.info("javaType " + javaType + " for " + hierarchyOfObjects[j]);
          if(javaType != null){
            //if the type is a list of that type, remove the List<>
            javaType = ((String)javaType).replaceAll("<", "").replaceAll(">", "").replaceAll("List", "");
            //build path to the embedded schema generated pojo
            String pojoPath = modelDirectory + "/" + javaType + ".java";
            //get fields in that pojo (may not be used)
            schemaFields2JavaTypes = JsonSchemaPojoUtil.jsonFields2Pojo(pojoPath);
            if(j == hierarchyOfObjects.length-2){
              String fieldInPojo = hierarchyOfObjects[hierarchyOfObjects.length-1];
              Set<String> fields2annotate = new HashSet<>();
              log.info("Adding annotation to " + fieldInPojo + " in " + pojoPath);
              fields2annotate.add(fieldInPojo);
              log.info("updating " + pojoPath + " with " + fields2annotate.size() + " annotations");
              if(!injectedAnnotations.contains(pojoPath+fields2annotate+annotation)){
                injectedAnnotations.add(pojoPath+fields2annotate+annotation);
                JsonSchemaPojoUtil.injectAnnotation(pojoPath, annotation, fields2annotate);
              }
            }
          }
        }
      }
    }
    log.info("updating " + rootPojo + " with " + annotateField4RootPojo.size() + " annotations, annotation list: ");
    annotateField4RootPojo.forEach(log::info);
    if(!injectedAnnotations.contains(rootPojo+annotation)){
      injectedAnnotations.add(rootPojo+annotation);
      JsonSchemaPojoUtil.injectAnnotation(rootPojo, annotation, new HashSet<>(annotateField4RootPojo));
    }
  }

  private void processRemainingSchemas(List<List<GlobalSchema>> globalUnprocessedSchemas,
      Set<String> processedPojos, String fieldName, Object fieldValue, String annotation) throws Exception {
    File modelDir = new File(outputDirectoryWithPackage + "/model/");
    if (! modelDir.exists()) {
      return;
    }
    File []pojos = modelDir.listFiles( (dir, name) -> name.endsWith(".java") );
    //loop over all pojos in the gen directory, check if the pojos have been processed
    //meaning mapped to a schema and annotated, if not, then process
    for (int k = 0; k < pojos.length; k++) {
      if(!processedPojos.contains(MODEL_PACKAGE_DEFAULT + "." + pojos[k].getName().replace(".java", ""))) {
        //get all fields in the pojo to compare them to all fields in each schema so that we
        //can map a pojo to a schema and then annotate the pojo's field in accordance with the schema
        Map<Object, Object> fieldsInPojo = JsonSchemaPojoUtil.jsonFields2Pojo(pojos[k].getAbsolutePath());
        int size1 = globalUnprocessedSchemas.size();
        //loop over all unprocessed schema across all ramls
        for (int l = 0; l < size1; l++) {
          List<GlobalSchema> gsList = globalUnprocessedSchemas.get(l);
          int size2 = gsList.size();
          //loop over all schemas for a specific raml
          for (int m = 0; m < size2; m++) {
            int counter = 0;
            GlobalSchema gs = gsList.get(m);
            List<String> schemaFields = JsonSchemaPojoUtil.getAllFieldsInSchema(new JsonObject(gs.value().value()));
            int size3 = schemaFields.size();
            //loop over all fields per schema and check if all fields exist in a specific pojo
            //if so, then we have mapped the pojo to the schema
            for (int n = 0; n < size3; n++) {
              if (   (fieldsInPojo.size() != schemaFields.size())
                  || (! fieldsInPojo.containsKey(schemaFields.get(n))) ) {
                break;
              }
              counter++;
            }
            if(counter == fieldsInPojo.size()){
              List<String> injectFieldList =
                  JsonSchemaPojoUtil.getFieldsInSchemaWithType(new JsonObject(gs.value().value()), fieldName, fieldValue);
              try {
                inject(pojos[k].getAbsolutePath(), annotation, injectFieldList);
              } catch (Exception e) {
                log.error(e.getMessage(), e);
              }
            }
          }
        }
      }
    }
  }

}
