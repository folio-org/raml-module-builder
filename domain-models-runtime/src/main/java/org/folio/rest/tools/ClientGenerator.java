package org.folio.rest.tools;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.folio.rest.RestVerticle;

import com.sun.codemodel.JBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JFieldVar;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JPackage;

/**
 *
 */
public class ClientGenerator {

  public static final String  PATH_ANNOTATION        = "javax.ws.rs.Path";
  public static final String  CLIENT_CLASS_SUFFIX    = "Client";
  public static final String  PATH_TO_GENERATE_TO    = "src/main/java/";

  /* Creating java code model classes */
  JCodeModel jCodeModel = new JCodeModel();

  /* for creating the class per interface */
  JDefinedClass jc = null;

  private String globalPath = null;

  private List<String> functionSpecificQueryParams = new ArrayList<>();

  private String className = null;

  private static Map<String, String> verbs = new HashMap<>();

  static {

    verbs.put("post", "post");
    verbs.put("get", "get");
    verbs.put("delete", "delete");
    verbs.put("put","put");
    verbs.put("options", "options");
    verbs.put("head","head");
    verbs.put("patch","patch");
    verbs.put("trace","trace");

  }

  public static void main(String[] args) throws Exception {

    AnnotationGrabber.generateMappings();

  }

  public void generateClassMeta(String className, Object globalPath){

    this.globalPath = "GLOBAL_PATH";

    /* Adding packages here */
    JPackage jp = jCodeModel._package(RTFConsts.CLIENT_GEN_PACKAGE);

    try {
      /* Giving Class Name to Generate */
      this.className = className.substring(RTFConsts.INTERFACE_PACKAGE.length()+1, className.indexOf("Resource"));
      jc = jp._class(this.className+CLIENT_CLASS_SUFFIX);

      /* class variable to root url path to this interface */
      JFieldVar globalPathVar = jc.field(JMod.PRIVATE | JMod.STATIC | JMod.FINAL, String.class, "GLOBAL_PATH");
      globalPathVar.init(JExpr.lit("/" + (String)globalPath));

      /* class variable tenant id */
      jc.field(JMod.PRIVATE, String.class, "tenantId");

      /* class variable to http options */
      jc.field(JMod.PRIVATE, HttpClientOptions.class, "options");

      /* class variable to http client */
      jc.field(JMod.PRIVATE, HttpClient.class, "httpClient");

      /* constructor, init the httpClient */
      JMethod consructor = jc.constructor(JMod.PUBLIC);
      consructor.param(String.class, "host");
      consructor.param(int.class, "port");
      consructor.param(String.class, "tenantId");

      /* populate constructor */
      JBlock conBody = consructor.body();
      conBody.directStatement("this.tenantId = tenantId;");
      conBody.directStatement("options = new HttpClientOptions();");
      conBody.directStatement("options.setLogActivity(true);");
      conBody.directStatement("options.setKeepAlive(true);");
      conBody.directStatement("options.setDefaultHost(host);");
      conBody.directStatement("options.setDefaultPort(port);");
      conBody.directStatement("httpClient = io.vertx.core.Vertx.vertx().createHttpClient(options);");

    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  public void generateMethodMeta(String methodName, JsonObject params, String url,
      String httpVerb, JsonArray contentType, JsonArray accepts){

    /* Adding method to the Class which is public and returns void */

    String conciseName = massageMethodName(methodName);

    JMethod jmCreate = jc.method(JMod.PUBLIC, void.class, conciseName);
    JBlock body = jmCreate.body();

    /* Adding java doc for method */
    jmCreate.javadoc().add("Service endpoint " + url);

    /* iterate on function params and add the relevant ones */
    boolean[] bufferUsed = new boolean[]{false};
    Iterator<Entry<String, Object>> paramList = params.iterator();
    functionSpecificQueryParams = new ArrayList<>();
    paramList.forEachRemaining(entry -> {
      String valueName = ((JsonObject) entry.getValue()).getString("value");
      String valueType = ((JsonObject) entry.getValue()).getString("type");
      String paramType = ((JsonObject) entry.getValue()).getString("param_type");
      if(handleParams(jmCreate, paramType, valueType, valueName)){
        bufferUsed[0] = true;
      }
    });

    body.directStatement("StringBuilder queryParams = new StringBuilder(\"?\");");
    int queryParamCount = functionSpecificQueryParams.size();
    boolean addAmp = false;
    for (int i = 0; i < queryParamCount; i++) {
      String qParam = functionSpecificQueryParams.get(i);
      if(i+1<queryParamCount){
        addAmp = true;
      }
      else{
        addAmp = false;
      }
      body.directStatement("if(((Object)"+qParam+").getClass().isPrimitive()) {queryParams.append(\""+qParam+"=\"+"+qParam+");");
      //    + "else{}");
      if(addAmp){
        body.directStatement("queryParams.append(\"&\");");
      }
      body.directStatement("}");
      body.directStatement("else if((Object)"+ qParam +" != null) {queryParams.append(\""+qParam+"=\"+"+qParam+");");
      //body.directStatement("if(!"+ qParam +" instancof Object) {queryParams.append(\""+qParam+"=\"+"+qParam+");}");
      if(addAmp){
        body.directStatement("queryParams.append(\"&\");");
      }
      body.directStatement("}");
    }
    /* create request */
    if(url == null){
      //if there is no path associated with a function
      //use the @path from the class
      url = globalPath;
    }
    else{
      /* replace {varName} with "+varName+" so that it will be replaced
       * in the url at runtime with the correct values */
      Matcher m = Pattern.compile("\\{.*?\\}").matcher(url);
      while(m.find()){
        String varName = m.group().replace("{","").replace("}", "");
        url = url.replace("{"+varName+"}", "\"+"+varName+"+\"");
      }

      url = "\""+url.substring(1)+"\"+queryParams.toString()";
    }

    body.directStatement("io.vertx.core.http.HttpClientRequest request = httpClient."+
        httpVerb.substring(httpVerb.lastIndexOf(".")+1).toLowerCase()+"("+url+");");

    body.directStatement("request.handler(responseHandler);");

    /* add content and accept headers if relevant */
    if(contentType != null){
      String cType = contentType.toString().replace("\"", "").replace("[", "").replace("]", "");
      body.directStatement("request.putHeader(\"Content-type\", \""+cType+"\");");
    }
    if(accepts != null){
      String aType = accepts.toString().replace("\"", "").replace("[", "").replace("]", "");
      body.directStatement("request.putHeader(\"Accept\", \""+aType+"\");");
    }

    /* push tenant id into x-okapi-tenant and authorization headers for now */
    body.directStatement("request.putHeader(\"Authorization\", tenantId);");
    body.directStatement("request.putHeader(\""+RestVerticle.OKAPI_HEADER_TENANT+"\", tenantId);");

    /* add response handler to each function */
    JClass handler = jCodeModel.ref(Handler.class).narrow(HttpClientResponse.class);
    jmCreate.param(handler, "responseHandler");

    if(bufferUsed[0]){
      body.directStatement("request.write(buffer);");
      body.directStatement("request.setChunked(true);");
    }
    body.directStatement("request.end();");

  }

  /**
   * @param methodName
   * @return
   */
  private String massageMethodName(String methodName) {
    int idx = methodName.lastIndexOf("By");
    if(idx == -1){
      //just remove the class name from the method
      //everything else should be concise enough
      return methodName.replaceFirst(this.className, "");
    }
    idx = idx+2;
    int redundantClassNameInFunction = methodName.indexOf(this.className);
    if(redundantClassNameInFunction == -1){
      return methodName;
    }
    //maintain the http method
    String httpVerb = methodName.substring(0, redundantClassNameInFunction);
    return httpVerb + methodName.substring(idx);
  }

  /**
   * @param paramType
   * @param valueType
   */
  private boolean handleParams(JMethod method, String paramType, String valueType, String valueName) {

    boolean bufferUsed = false;

    if (AnnotationGrabber.NON_ANNOTATED_PARAM.equals(paramType) /*&& !FILE_UPLOAD_PARAM.equals(valueType)*/) {
      try {
        // this will also validate the json against the pojo created from the schema
        Class<?> entityClazz = Class.forName(valueType);

        if (!valueType.equals("io.vertx.core.Handler") && !valueType.equals("io.vertx.core.Context") &&
            !valueType.equals("java.util.Map")) {

          /* this is a post or put since our only options here are receiving a reader (data in body) or
           * entity - which is also data in body - but we can only have one since a multi part body
           * should be indicated by a multipart object ? */
          JBlock methodBody = method.body();
          methodBody.directStatement( "io.vertx.core.buffer.Buffer buffer = io.vertx.core.buffer.Buffer.buffer();" );

          if("java.io.Reader".equals(valueType)){
            method.param(String.class, "arg0");
            methodBody.directStatement( "buffer.appendString(arg0);" );
          }
          else{
            methodBody.directStatement( "buffer.appendString("
                + "org.folio.rest.persist.MongoCRUD.entity2Json("+entityClazz.getSimpleName()+").encode());");
            method.param(entityClazz, entityClazz.getSimpleName());
          }

          bufferUsed = true;

        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    else if (AnnotationGrabber.PATH_PARAM.equals(paramType)) {
      method.param(String.class, valueName);
    }
    else if (AnnotationGrabber.HEADER_PARAM.equals(paramType)) {
      method.param(String.class, valueName);
    }
    else if (AnnotationGrabber.QUERY_PARAM.equals(paramType)) {
      // support enum, numbers or strings as query parameters
      try {
        if (valueType.contains("String")) {
          method.param(String.class, valueName);
        } else if (valueType.contains("int")) {
          method.param(int.class, valueName);
        } else if (valueType.contains("boolean")) {
          method.param(boolean.class, valueName);
        } else if (valueType.contains("BigDecimal")) {
          method.param(BigDecimal.class, valueName);
        } else { // enum object type
          try {
            String enumClazz = replaceLast(valueType, ".", "$");
            Class<?> enumClazz1 = Class.forName(enumClazz);
            if (enumClazz1.isEnum()) {
              method.param(enumClazz1, valueName);
            }
          } catch (Exception ee) {
            ee.printStackTrace();
          }
        }
        functionSpecificQueryParams.add(valueName);
      }
      catch(Exception e){
        e.printStackTrace();
      }
    }

    return bufferUsed;
  }

  public void generateClass() throws IOException{
    /* Building class at given location */
    jCodeModel.build(new File(PATH_TO_GENERATE_TO));
  }

  private static String replaceLast(String string, String substring, String replacement) {
    int index = string.lastIndexOf(substring);
    if (index == -1)
      return string;
    return string.substring(0, index) + replacement + string.substring(index + substring.length());
  }
}
