package org.folio.rest.tools;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.raml.v2.api.RamlModelBuilder;
import org.raml.v2.api.RamlModelResult;
import org.raml.v2.api.model.v08.api.GlobalSchema;

import com.github.javaparser.ast.Node;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.visitor.ModifierVisitor;

import io.vertx.core.json.JsonObject;

/**
 * Can read a RAML file and its JSON schemas and annotate the corresponding
 * Pojo .java file with the annotations as specified in the schemas.
 */
public class JsonSchemaPojoUtil {
  /**
   * inject an annotation into each of the fields indicated in the Set<String>.
   * @param path2pojo - full path to the .java file to inject into
   * @param annotationName - name of annotation - for example javax.validation.constraints.Null
   * @param fields - fields in the pojo to annotate
   * @throws Exception
   */
  public static void injectAnnotation(String path2pojo, String annotationName, Set<String> fields) throws Exception{
    com.github.javaparser.ast.CompilationUnit cu = com.github.javaparser.JavaParser.parse(
      new FileInputStream(path2pojo));
    ModifierVisitor<?> annotationVisitor = new PojoModifier(annotationName, fields);
    annotationVisitor.visit(cu, null);
    BufferedWriter bw = Files.newBufferedWriter(new File(path2pojo).toPath(), StandardCharsets.UTF_8);
    bw.write(cu.toString());
    bw.close();
  }

  /**
   * will return a map containing the json schema field name and the java field type generated for that
   * json schema field -
   * @param path2pojo
   * @return
   * @throws Exception
   */
  public static Map<Object, Object> jsonFields2Pojo(String path2pojo) throws Exception{
    com.github.javaparser.ast.CompilationUnit cu = com.github.javaparser.JavaParser.parse(
      new FileInputStream(path2pojo));
    Map<Object, Object> jsonField2PojoMap = new HashMap<>();
    ModifierVisitor<?> annotationVisitor = new AnnotationExtractor("JsonProperty", jsonField2PojoMap);
    annotationVisitor.visit(cu, null);
    return jsonField2PojoMap;
  }

  private static class PojoModifier extends ModifierVisitor<Void> {

    private String annotationName;
    private Set<String> fields;
      /**
     * @param annotationName
     * @param fields
     */
    public PojoModifier(String annotationName, Set<String> fields) {
      this.annotationName = annotationName;
      this.fields = fields;
    }

      @Override
      public FieldDeclaration visit(FieldDeclaration fd, Void arg) {
          super.visit(fd, arg);
          boolean []addAnno = new boolean[]{false};
          fd.getAnnotations().forEach( annotation -> {
            try {
              List<Node> annos = annotation.getChildNodes();
              if(annos.size() == 2 &&  "JsonProperty".equalsIgnoreCase(annos.get(0).toString())){
                if(this.fields.contains(annos.get(1).toString().replaceAll("\"", ""))){
                  //we found the field in the pojo that matches the field we need to inject into
                  //add fully qualified annotation ex. javax.validation.constraints.Null
                  addAnno[0] = true;
                }
              }
            } catch (java.util.NoSuchElementException e) {
              System.out.println("");
            }
          });
          if(addAnno[0]){
            fd.addAnnotation(annotationName);
          }
          return fd;
      }
  }

  private static class AnnotationExtractor extends ModifierVisitor<Void> {

    private String annotationName;
    private Map<Object, Object> jsonField2PojoMap = new HashMap<>();
      /**
     * @param annotationName
     * @param fields
     */
    public AnnotationExtractor(String annotationName, Map<Object, Object> jsonField2PojoMap) {
      this.annotationName = annotationName;
      this.jsonField2PojoMap = jsonField2PojoMap;
    }

    @Override
    public FieldDeclaration visit(FieldDeclaration fd, Void arg) {
      super.visit(fd, arg);
      boolean []addAnno = new boolean[]{false};
      fd.getAnnotations().forEach( annotation -> {
        List<Node> annos = annotation.getChildNodes();
        if(annos.size() == 2 &&  annotationName.equalsIgnoreCase(annos.get(0).toString())){
          //attempt to get parent node of this annotation , which will be the field node
          //containing the comment, annotations, and the needed field name itself
          Optional<Node> n = annotation.getParentNode();
          if(n.isPresent()){
            //get list of nodes, the field name should be the last node in the list
            List<Node> list = n.get().getChildNodes();
            int size = list.size();
            if(size > 0){
              //the field node itself is a list of type, name
              List<Node> fieldInfo = list.get(size-1).getChildNodes();
              jsonField2PojoMap.put(annos.get(1).toString().replaceAll("\"", ""), fieldInfo.get(0).toString());
            }
          }
        }
      });
      if(addAnno[0]){
        fd.addAnnotation(annotationName);
      }
      return fd;
    }
  }

  /**
   * Pass in a json schema (as a jsonObject type) - from the raml - this can be done using the following
   *    RamlModelResult ramlModelResult = new RamlModelBuilder().buildApi("....raml");
   *    List<GlobalSchema> schema = ramlModelResult.getApiV08().schemas();
   * @param schema - the schema
   * @param type - type to look for - "type" / "readonly" / etc...
   * @param value - the value of the type - true / "string" / etc...
   * @return - returns a list of paths within the schema that contain this type = value , the path
   * is dot separated - so for embedded objects in the schema you would be something like a.b.c
   */
  public static List<String> getFieldsInSchemaWithType(JsonObject schema, String type, Object value){
    if(schema == null){
      return null;
    }
    Set<String> paths = new HashSet<>();
    Map<String, Object> map = schema.getMap();
    map.forEach((k,v)->collectNodes(paths, new StringBuffer() , k , v, type, value, false));
    return new ArrayList<>(paths);
  }

  public static List<String> getAllFieldsInSchema(JsonObject schema){
    if(schema == null){
      return null;
    }
    Set<String> paths = new HashSet<>();
    Map<String, Object> map = schema.getMap();
    map.forEach((k,v)->collectNodes(paths, new StringBuffer() , k , v, "*", null, false));
    return new ArrayList<>(paths);
  }

  public static List<GlobalSchema> getSchemasFromRaml(File path2Raml) {
    RamlModelResult ramlModelResult = new RamlModelBuilder().buildApi(path2Raml.getAbsolutePath());
    //get a list of schemas from the raml
    List<GlobalSchema> schemaListInRAML = ramlModelResult.getApiV08().schemas();
    return schemaListInRAML;
  }

  private static void collectNodes(Set<String> paths, StringBuffer sb, String path, final Object value, String compare2type, Object compare2Val, boolean inArray){
    if(path.equals("required")){
      return;
    }
    if(value instanceof Map){
      if(path.equals("properties") || path.equals("items")){
        //objects, need to parse them recursively
        Map<String, Object> map = (Map<String, Object>) value;
        map.forEach((k,v)->collectNodes(paths, sb, k , v, compare2type, compare2Val, false));
      }
      else{
        if(!path.startsWith("$")){
          //don't add objects like $date to the field path as they are descriptive
          sb.append(path).append(".");
        } else{
          //but we need to add $ fields otherwise we lose track of hierarchy
          //so place a dummy value and then remove it
          sb.append("$$$$").append(".");
        }
        Map<String, Object> map = (Map<String, Object>) value;
        map.forEach((k,v)->collectNodes(paths, sb, k , v, compare2type, compare2Val, "array".equals(((Map)value).get("type"))));
        removePreviousLevel(sb);
      }
    }
    else {
      //we are here because this is a node of type object and we are now looking at the
      //individual properties
      //if("link".equalsIgnoreCase(path)){
      //  System.out.println("PATH " + path + " , " + sb.toString());
      //}
      if((path.equals(compare2type) && value.equals(compare2Val)) || compare2type.equals("*") ||
          (path.equals(compare2type) && value.equals("*"))){
        if(sb.length() > 0){
          paths.add(sb.toString().substring(0, sb.length()-1).replace(".$$$$", ""));
        }
      }
    }
  }

  private static void removePreviousLevel(StringBuffer sb){
    if(sb.length() == 0){
      return;
    }
    String str = sb.toString();
    String[] levels = str.split("\\.");
    String remove = levels[levels.length-1]+".";
    sb.delete(0, sb.length());
    sb.append(str.substring(0, str.lastIndexOf(remove)));
  }


}
