package org.folio.rest.tools;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;

import org.folio.rest.tools.Raml2Java;
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
  static final GeneratorProxy generator = new GeneratorProxy();

  public static void main(String args[]) throws Exception{

    List<GeneratorExtension> extensions = new ArrayList<>();
    extensions.add(new Raml2Java());

    String basedir = System.getProperties().getProperty("project.basedir");

    outputDirectory = basedir + ClientGenerator.PATH_TO_GENERATE_TO;

    String outputDirectoryWithPackage = outputDirectory + PACKAGE_DEFAULT.replace('.', '/');

    ClientGenerator.makeCleanDir(outputDirectoryWithPackage);

    configuration = new Configuration();
    configuration.setJaxrsVersion(JaxrsVersion.JAXRS_2_0);
    configuration.setUseJsr303Annotations(true);
    configuration.setJsonMapper(AnnotationStyle.JACKSON2);
    configuration.setBasePackageName(PACKAGE_DEFAULT);
    configuration.setExtensions(extensions);

    //outputDirectory = System.getProperty("output_directory");

/*    if(outputDirectory == null){
      outputDirectory = System.getProperty("java.io.tmpdir");
    }

    if(inputDirectory == null){
      throw new Exception("unable to run java generation process without a valid input directory of the raml content");
    }*/

    inputDirectory  = System.getProperty("raml_files");
    //inputDirectory  = "C:\\Git\\circulation\\ramls\\circulation";// "C:\\Git\\raml-module-builder\\domain-models-api-interfaces\\src\\main\\resources\\raml";
    if(inputDirectory == null){

      inputDirectory = basedir + SOURCES_DEFAULT;

    }

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
    for (int i = 0; i < ramls.length; i++) {
      BufferedReader reader=new BufferedReader(new FileReader(ramls[i]));
      String line=reader.readLine();
      reader.close();
      if(line.startsWith("#%RAML")) {
        System.out.println("processing " + ramls[i]);
        generator.run(new FileReader(ramls[i]), configuration, ramls[i].getAbsolutePath());
        numMatches++;
      }
      else{
        System.out.println(ramls[i] + " has a .raml suffix but does not start with #%RAML");
      }
    }
    System.out.println("processed: " + numMatches + " raml files");

    return;

  }

}
