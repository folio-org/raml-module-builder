package org.folio.rulez;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
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

import org.apache.commons.io.IOUtils;
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

  private String                  kieFileSystemPath     = "src/main/resources";
  private URI                     externalRulesDir      = null;
  private KieContainer            kieContainer          = null;
  private KieSession              ksession              = null;
  private KieServices             kieServices           = null;
  private KieFileSystem           kfs                   = null;
  private KieBuilder              kieBuilder            = null;

  private List<String>            rules                 = null;

  /**
   * will look for rules in the default /resources/rules path in the jar
   */
  public Rules() {
  }

  /**
   * will set rules to a directory on the file system
   * @param rulesDirPath
   */
  public Rules(String rulesDirPath) {
    if(rulesDirPath != null){
      externalRulesDir =  new File(rulesDirPath).toURI();
    }
  }

  /**
   * Each String in the list should be a valid .drl file
   * the entire list will be loaded into the new session.
   * this can be used to read rules saved in the DB
   * @param ruleContent
   */
  public Rules(List<String> ruleContent) {
    rules = ruleContent;
  }

  /**
   * Load all .drl files
   *
   * if a rule does not compile an exception will be thrown indicating the problem
   *
   * @param path
   * @return - session containing all loaded rules
   * @throws Exception
   */
  public KieSession buildSession() throws Exception {
    kieServices = KieServices.Factory.get();
    KieRepository kr = kieServices.getRepository();
    kfs = kieServices.newKieFileSystem();
    if(rules != null){
      loadFromList();
    }
    else{
      loadFromFiles();
    }
    kieBuilder = kieServices.newKieBuilder( kfs ).buildAll();
    Results results = kieBuilder.getResults();
    if( results.hasMessages( Message.Level.ERROR ) ){
        System.out.println( results.getMessages() );
        throw new IllegalStateException( results.getMessages() + "" );
    }
    kieContainer =
        kieServices.newKieContainer( kr.getDefaultReleaseId() );
    ksession = kieContainer.newKieSession();
    ksession.addEventListener(new RuleTracker());
    return ksession;
  }

  /**
   * @throws IOException
   *
   */
  private void loadFromList() throws IOException {
    int size = rules.size();
    for (int i = 0; i < size; i++) {
      kieFileSystemPath = kieFileSystemPath + RULES_DIR_JAR + "/" + i + ".drl";
      kfs.write( kieFileSystemPath , kieServices.getResources().newInputStreamResource(
        IOUtils.toInputStream( rules.get(i), "UTF-8") ));
    }
  }

  private void loadFromFiles() throws Exception {
    try {
      if(externalRulesDir == null){
        externalRulesDir = Rules.class.getResource(RULES_DIR_JAR).toURI();
      }
      List<String> ruleFiles = getRules(externalRulesDir);
      for (int i = 0; i < ruleFiles.size(); i++) {
        InputStream fis = null;
        if (externalRulesDir.getScheme().equals("jar")) {
          fis = getClass().getResourceAsStream(RULES_DIR_JAR + "/" + ruleFiles.get(i));
          kieFileSystemPath = kieFileSystemPath + RULES_DIR_JAR + "/" + ruleFiles.get(i);
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
    } catch (NullPointerException e) {
      //TODO log this!
       System.out.println("no rules directory found, continuing...");
    }
  }

  public static List<String> validateRules(InputStream resource) throws Exception {

    VerifierBuilder vBuilder = VerifierBuilderFactory.newVerifierBuilder();
    Verifier verifier = vBuilder.newVerifier();
    verifier.addResourcesToVerify(new InputStreamResource(resource, "UTF8"), ResourceType.DRL);
    List<String> errorList = new ArrayList<>();
    for (int i = 0; i < verifier.getErrors().size(); i++)
    {
      errorList.add(verifier.getErrors().get(i).getMessage());
    }
    return errorList;
  }


  private ArrayList<String> getRules(URI uri) throws Exception {

    System.out.println("Getting rules from " + uri.toString());
    Path rulePath = null;
    ArrayList<String> list = new ArrayList<>();
    FileSystem fileSystem = null;

    if (uri.getScheme().equals("jar")) {
      try {
        fileSystem = FileSystems.newFileSystem(uri, Collections.<String, Object> emptyMap());
      } catch (FileSystemAlreadyExistsException e) {
        fileSystem = FileSystems.getFileSystem(uri);
      }
      rulePath = fileSystem.getPath(RULES_DIR_JAR);
    }
    else if(uri.isAbsolute()){
      rulePath = Paths.get(uri);
    }
    else {
      uri = Rules.class.getClassLoader().getResource(RULES_DIR_IDE).toURI();
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
