package org.folio.rest.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;

import org.jsonschema2pojo.AnnotationStyle;
import org.raml.jaxrs.codegen.core.Configuration;
import org.raml.jaxrs.codegen.core.Configuration.JaxrsVersion;
import org.raml.jaxrs.codegen.core.GeneratorProxy;
import org.raml.jaxrs.codegen.core.ext.GeneratorExtension;

/**
 * @author shale
 *
 */
public class GenerateRunner {

  private static String outputDirectory = null;
  private static String inputDirectory = null;
  private static Configuration configuration = null;

  private static final String PACKAGE_DEFAULT = "org.folio.rest.jaxrs";
  private static final String SOURCES_DEFAULT = "/ramls/";
  private static final String RESOURCE_DEFAULT = "/target/classes";

  static final GeneratorProxy generator = new GeneratorProxy();

  public static void main(String args[]) throws Exception{

    List<GeneratorExtension> extensions = new ArrayList<>();
    extensions.add(new Raml2Java());

    String root = System.getProperties().getProperty("project.basedir");

    outputDirectory = root + "/src/main/java/";// + PACKAGE_DEFAULT.replace('.', '/');

    String outputDirectoryWithPackage = outputDirectory + PACKAGE_DEFAULT.replace('.', '/');

    ClientGenerator.makeCleanDir(outputDirectoryWithPackage);

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
          generator.run(new FileReader(ramls[j]), configuration, ramls[j].getAbsolutePath());
          numMatches++;
        }
        else{
          System.out.println(ramls[j] + " has a .raml suffix but does not start with #%RAML");
        }
      }
      System.out.println("processed: " + numMatches + " raml files");
    }
    return;

  }

}
