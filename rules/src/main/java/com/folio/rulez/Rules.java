package com.folio.rulez;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.drools.core.io.impl.InputStreamResource;
import org.drools.verifier.Verifier;
import org.drools.verifier.builder.VerifierBuilder;
import org.drools.verifier.builder.VerifierBuilderFactory;
import org.kie.api.KieServices;
import org.kie.api.builder.KieBuilder;
import org.kie.api.builder.KieFileSystem;
import org.kie.api.builder.KieRepository;
import org.kie.api.builder.Message;
import org.kie.api.builder.Results;
import org.kie.api.io.ResourceType;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;


/**
 * This is a class to launch rules.
 * Optionally pass a directory of .drl files to the Rules constructor.
 * Call the buildSession() function to
 * scan all the .drl files (only) in that directory - the files are validated as correct drools (if not
 * an exception is thrown with an informative message) - the files are added to the ksession.
 * The session can now be used to run drools - see junit tests in this project for an example.
 * dispose() of the session when done
 * 
 * by default - .drl files placed in the /resources/rules directory of the project will be loaded
 * if no path is passed
 */
public class Rules {

  public static final String      RULES_DIR_JAR         = "/rules";
  public static final String      RULES_DIR_IDE         = "rules";

  
  private URI                     externalRulesDir      = null;        
  
 
  public Rules() {
  }
  
  public Rules(String rulesDirPath) {
    if(rulesDirPath != null){
      externalRulesDir =  new File(rulesDirPath).toURI();
    }
  }
  
  /**
   * Load all .drl files from the specified directory (URI) 
   * create a kiefilesystem (an in memory file system with a key (path) - value (input stream) file system
   * note that the key should have the /src/main/resources prefix
   * 
   * if a rule does not compile an exception will be thrown indicating the problem
   *  
   * @param path
   * @return - session containing all loaded rules
   * @throws Exception
   */
  public KieSession buildSession(URI path) throws Exception {
    KieServices kieServices = KieServices.Factory.get();
    KieRepository kr = kieServices.getRepository();
    KieFileSystem kfs = kieServices.newKieFileSystem();
    List<String> ruleFiles = getRules(path);
    for (int i = 0; i < ruleFiles.size(); i++) {
      InputStream fis = null;
      String kieFileSystemPath = "/src/main/resources";
      if (path.getScheme().equals("jar")) {
        fis = getClass().getResourceAsStream(RULES_DIR_JAR + "/" + ruleFiles.get(i));
        kieFileSystemPath = ruleFiles.get(i);
      }else{
        /* build a virtual path in the kie file system to each drools drl file by taking the
         * name of the drool file and the name of the parent directory of the drool file and
         * putting them together abc/def.drl <- low potential of rule collision - handle better TODO */
        File file = new File( ruleFiles.get(i) );
        File parent = file.getParentFile();
        if(parent != null){
          kieFileSystemPath = kieFileSystemPath + "/" + parent.getName() + "/" + file.getName();      
        }else{
          kieFileSystemPath = kieFileSystemPath + "/drools/" + file.getName();  
        }
        fis = new FileInputStream(file);
      }
      kfs.write( kieFileSystemPath , kieServices.getResources().newInputStreamResource( fis ) );
      System.out.println("loading " + ruleFiles.get(i));
    }
    KieBuilder kieBuilder = kieServices.newKieBuilder( kfs ).buildAll();
    Results results = kieBuilder.getResults();
    if( results.hasMessages( Message.Level.ERROR ) ){
        System.out.println( results.getMessages() );
        throw new IllegalStateException( results.getMessages() + "" );
    }
    KieContainer kieContainer =
        kieServices.newKieContainer( kr.getDefaultReleaseId() );
    KieSession ksession = kieContainer.newKieSession();
    ksession.addEventListener(new RuleTracker());
    return ksession;
  }

  public KieSession buildSession() throws Exception {
    try {
      if(externalRulesDir == null){
        externalRulesDir = Rules.class.getResource(RULES_DIR_JAR).toURI();
      }
      return buildSession(externalRulesDir);
    } catch (NullPointerException e) {
      //TODO log this!
       System.out.println("no rules directory found, continuing...");
    }
    return null;
  }
 
  public static List<String> validateRules(InputStream resource) throws Exception {

    VerifierBuilder vBuilder = VerifierBuilderFactory.newVerifierBuilder();
    Verifier verifier = vBuilder.newVerifier();
    verifier.addResourcesToVerify(new InputStreamResource(resource, "UTF8"), ResourceType.DRL);
    List<String> errorList = new ArrayList<String>();
    for (int i = 0; i < verifier.getErrors().size(); i++)
    {
      errorList.add(verifier.getErrors().get(i).getMessage());
    }
    return errorList;
  }

  
  private ArrayList<String> getRules(URI uri) throws Exception {

    Path rulePath = null;
    ArrayList<String> list = new ArrayList<String>();
    FileSystem fileSystem = null;
    
    if (!uri.isAbsolute()) {
      fileSystem = null;
      if (uri.getScheme().equals("jar")) {
        try {
          fileSystem = FileSystems.newFileSystem(uri, Collections.<String, Object> emptyMap());
        } catch (FileSystemAlreadyExistsException e) {
          fileSystem = FileSystems.getFileSystem(uri);
          //e.printStackTrace();
        }
        rulePath = fileSystem.getPath(RULES_DIR_JAR);
      } else {
        uri = Rules.class.getClassLoader().getResource(RULES_DIR_IDE).toURI();
        rulePath = Paths.get(uri);
      }
    }
    else{
      rulePath = Paths.get(uri); 
    }
    Stream<Path> walk = Files.walk(rulePath, 1);
    for (Iterator<Path> it = walk.iterator(); it.hasNext();) {
      Path file = it.next();
      String name = file.getFileName().toString();
      if(name.endsWith("drl")){
        if (uri.getScheme().equals("jar")) {
          list.add(name);
        }else{
          list.add(file.toAbsolutePath().toString());
        }
      }
    }
    walk.close();
    if (fileSystem != null) {
      fileSystem.close();
    }
    return list;
  }

}
