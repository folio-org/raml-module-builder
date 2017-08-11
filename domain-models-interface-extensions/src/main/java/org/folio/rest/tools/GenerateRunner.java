package org.folio.rest.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.folio.rest.utils.GlobalSchemaPojoMapperCache;
import org.jsonschema2pojo.AnnotationStyle;
import org.raml.jaxrs.codegen.core.Configuration;
import org.raml.jaxrs.codegen.core.Configuration.JaxrsVersion;
import org.raml.jaxrs.codegen.core.GeneratorProxy;
import org.raml.jaxrs.codegen.core.ext.GeneratorExtension;
import org.raml.v2.api.RamlModelBuilder;
import org.raml.v2.api.RamlModelResult;
import org.raml.v2.api.model.v08.api.GlobalSchema;

import io.vertx.core.json.JsonObject;

/**
 * @author shale
 *
 */
public class GenerateRunner {

  static final GeneratorProxy GENERATOR = new GeneratorProxy();

  private static String outputDirectory = null;
  private static String inputDirectory = null;
  private static Configuration configuration = null;

  private static final String PACKAGE_DEFAULT = "org.folio.rest.jaxrs";
  private static final String SOURCES_DEFAULT = "/ramls/";
  private static final String RESOURCE_DEFAULT = "/target/classes";

  public static void main(String args[]) throws Exception{

    List<GeneratorExtension> extensions = new ArrayList<>();
    extensions.add(new Raml2Java());

    String root = System.getProperties().getProperty("project.basedir");

    outputDirectory = root + "/src/main/java/";// + PACKAGE_DEFAULT.replace('.', '/');

    String outputDirectoryWithPackage = outputDirectory + PACKAGE_DEFAULT.replace('.', '/');

    ClientGenerator.makeCleanDir(outputDirectoryWithPackage);

    //if we are generating interfaces, we need to remove any generated client code
    //as if the interfaces have changed in a way (pojos removed, etc...) that causes
    //the client generated code to cause compilation errors
    String clientDir = System.getProperties().getProperty("project.basedir")
        + ClientGenerator.PATH_TO_GENERATE_TO
        + RTFConsts.CLIENT_GEN_PACKAGE.replace('.', '/');
    ClientGenerator.makeCleanDir(clientDir);


    configuration = new Configuration();
    configuration.setJaxrsVersion(JaxrsVersion.JAXRS_2_0);
    configuration.setUseJsr303Annotations(true);
    configuration.setJsonMapper(AnnotationStyle.JACKSON2);
    configuration.setBasePackageName(PACKAGE_DEFAULT);
    configuration.setExtensions(extensions);

    String ramlsDir = System.getProperty("raml_files");
    if(ramlsDir == null) {
      ramlsDir = root + SOURCES_DEFAULT;
    }

    //copy ramls dir to /target so it is in the classpath. this is needed
    //for the criteria object to check data types of paths in a json by
    //checking them in the schema. will probably be further needed in the future

    String[] paths = ramlsDir.split(","); //if multiple paths are indicated with a , delimiter
    for (int i = 0; i < paths.length; i++) {
      String rootPath2RamlDir = Paths.get(paths[i]).getFileName().toString();
      System.out.println("copying ramls to target directory at: " + (root+RESOURCE_DEFAULT+"/"+rootPath2RamlDir));
      FileUtils.copyDirectory(new File(paths[i]), new File(root+RESOURCE_DEFAULT+"/"+rootPath2RamlDir));
    }

    String []dirs = ramlsDir.split(",");
    for (int i = 0; i < dirs.length; i++) {
      inputDirectory = dirs[i];

      System.out.println( "Input directory " + inputDirectory);

      configuration.setOutputDirectory(new File(outputDirectory));
      configuration.setSourceDirectory(new File(inputDirectory));

      int numMatches = 0;

      if(!new File(inputDirectory).isDirectory()){
        System.out.println(inputDirectory + " is not a valid directory");
      }

      File []ramls = new File(inputDirectory).listFiles(new FilenameFilter() {

        @Override
        public boolean accept(File dir, String name) {
          if(name.endsWith(".raml")){
            return true;
          }
          return false;
        }
      });
      for (int j = 0; j < ramls.length; j++) {
        BufferedReader reader=new BufferedReader(new FileReader(ramls[j]));
        String line=reader.readLine();
        reader.close();
        if(line.startsWith("#%RAML")) {
          System.out.println("processing " + ramls[j]);
          //generate java interfaces and pojos from raml
          GENERATOR.run(new FileReader(ramls[j]), configuration, ramls[j].getAbsolutePath());
          numMatches++;

          RamlModelResult ramlModelResult = new RamlModelBuilder().buildApi(ramls[j].getAbsolutePath());
          List<GlobalSchema> schemaList = ramlModelResult.getApiV08().schemas();

          //get list of schema to injectedFieldList
          //have a map of top level schemas (schemas used in the raml schema:, etc...) to pojos
          //scan fields in top level pojo list to get referenced pojos
          //the name of their schema will be the jsonproperty annotation name
          //check if for the top level pojo , the embedded objects need annotating

          //contains schemas referenced in the raml (not embedded ones) - to pojo mappings
          Set<Entry<Object, Object>> o = GlobalSchemaPojoMapperCache.getSchema2PojoMapper().entrySet();
          o.forEach( entry -> {
            String schema = FilenameUtils.getName(((URL)entry.getKey()).getPath()); // schema
            String pojo = (String)entry.getValue(); //pojo generated from that schema
            int schemasSize = schemaList.size();
            for (int l = 0; l < schemasSize; l++) {
              //System.out.println("comparing " + schema + " to " + schemaList.get(l).key());
              if(schemaList.get(l).key().equalsIgnoreCase(schema)){
                //get the fields in the schema that contain the field name we are looking for
                List<String> injectFieldList =
                    JsonSchemaPojoUtil.getNodesWithType(new JsonObject(schemaList.get(l).value().value()), "readonly", true);
                String fullPathPojo = outputDirectory + pojo.replace('.', '/') + ".java";
                System.out.println("-->schema: " + schema + ", pojo: " + fullPathPojo + ", ");
                System.out.println("Schema content " + schemaList.get(l).value().value());
                injectFieldList.stream().forEach(System.out::println);
                try {
                  System.out.println("updating " + fullPathPojo + " with " + injectFieldList.size() + " annotations");
                  //JsonSchemaPojoUtil.injectAnnotation(fullPathPojo, "javax.validation.constraints.Null", new HashSet(injectFieldList));
                } catch (Exception e) {
                  e.printStackTrace();
                }
              }
            }
          });
/*          RamlModelResult ramlModelResult = new RamlModelBuilder().buildApi(ramls[j].getAbsolutePath());
          //get a list of schemas from the raml
          List<GlobalSchema> schemaListInRAML = ramlModelResult.getApiV08().schemas();
          Set<GlobalSchema> ramlSchemaSet = new HashSet<>(schemaListInRAML);
          int schemasSizeInRAML = schemaListInRAML.size();
          Map<Object, Object> topLevelSchemas = GlobalSchemaPojoMapperCache.getSchema2PojoMapper();
          ramlSchemaSet.forEach( gSchema -> {
            System.out.println(gSchema.key());
          });
          topLevelSchemas.forEach( (schema , pojo) -> {
            System.out.println("-->schema: " + schema + ", pojo: " + pojo + ", ");
            try {
              processPojoAndSchema((String)pojo , (String)schema , schemaListInRAML, topLevelSchemas);
            } catch (Exception e) {
              e.printStackTrace();
            }
          });*/

        }
        else{
          System.out.println(ramls[j] + " has a .raml suffix but does not start with #%RAML");
        }
      }
      System.out.println("processed: " + numMatches + " raml files");
    }
    return;
  }


  public static boolean isPrimitiveOrPrimitiveWrapperOrString(String type) {
    return type.equalsIgnoreCase("Double") ||type.equalsIgnoreCase("Float") || type.equalsIgnoreCase("Long")
        || type.equalsIgnoreCase("Integer") || type.equalsIgnoreCase("Short") ||
        type.equalsIgnoreCase("Character") || type.equalsIgnoreCase("char") ||
        type.equalsIgnoreCase("Byte") || type.equalsIgnoreCase("Boolean") ||
        type.equalsIgnoreCase("String") || type.equalsIgnoreCase("int");
}
}
