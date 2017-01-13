package org.folio.rest.tools;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientOptions;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.mail.MessagingException;
import javax.mail.internet.MimeMultipart;

import com.sun.codemodel.JBlock;
import com.sun.codemodel.JCatchBlock;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JDocComment;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JExpression;
import com.sun.codemodel.JFieldVar;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JPackage;
import com.sun.codemodel.JTryBlock;
import com.sun.codemodel.JVar;
import java.io.UnsupportedEncodingException;

/**
 *
 */
public class ClientGenerator {

  public static final String  PATH_ANNOTATION        = "javax.ws.rs.Path";
  public static final String  CLIENT_CLASS_SUFFIX    = "Client";
  public static final String  PATH_TO_GENERATE_TO    = "/src/main/java/";
  public static final String  OKAPI_HEADER_TENANT = "x-okapi-tenant";

  /* Creating java code model classes */
  JCodeModel jCodeModel = new JCodeModel();

  /* for creating the class per interface */
  JDefinedClass jc = null;

  private String globalPath = null;

  private List<String> functionSpecificHeaderParams = new ArrayList<>();

  private String className = null;

  private String mappingType = "postgres";

  public static void main(String[] args) throws Exception {

    AnnotationGrabber.generateMappings();

  }

  public void generateClassMeta(String className, Object globalPath){

    String mapType = System.getProperty("json.type");
    if(mapType != null){
      if(mapType.equals("mongo")){
        mappingType = "mongo";
      }
    }
    this.globalPath = "GLOBAL_PATH";

    /* Adding packages here */
    JPackage jp = jCodeModel._package(RTFConsts.CLIENT_GEN_PACKAGE);

    try {
      /* Giving Class Name to Generate */
      this.className = className.substring(RTFConsts.INTERFACE_PACKAGE.length()+1, className.indexOf("Resource"));
      jc = jp._class(this.className+CLIENT_CLASS_SUFFIX);
      JDocComment com = jc.javadoc();
      com.add("Auto-generated code - based on class " + className);
      
      /* class variable to root url path to this interface */
      JFieldVar globalPathVar = jc.field(JMod.PRIVATE | JMod.STATIC | JMod.FINAL, String.class, "GLOBAL_PATH");
      globalPathVar.init(JExpr.lit("/" + (String)globalPath));

      /* class variable tenant id */
      JFieldVar tenantId = jc.field(JMod.PRIVATE, String.class, "tenantId");

      /* class variable to http options */
      JFieldVar options = jc.field(JMod.PRIVATE, HttpClientOptions.class, "options");

      /* class variable to http client */
      jc.field(JMod.PRIVATE, HttpClient.class, "httpClient");

      /* constructor, init the httpClient - allow to pass keep alive option */
      JMethod consructor = jc.constructor(JMod.PUBLIC);
      JVar host = consructor.param(String.class, "host");
      JVar port = consructor.param(int.class, "port");
      JVar param = consructor.param(String.class, "tenantId");
      JVar keepAlive = consructor.param(boolean.class, "keepAlive");

     /* populate constructor */
      JBlock conBody=  consructor.body();
      conBody.assign(JExpr._this().ref(tenantId), param);
      conBody.assign(options, JExpr._new(jCodeModel.ref(HttpClientOptions.class)));
      conBody.invoke(options, "setLogActivity").arg(JExpr.TRUE);
      conBody.invoke(options, "setKeepAlive").arg(keepAlive);
      conBody.invoke(options, "setDefaultHost").arg(host);
      conBody.invoke(options, "setDefaultPort").arg(port);
      conBody.decl(jCodeModel._ref(io.vertx.core.Context.class), "context",
              JExpr.direct("io.vertx.core.Vertx.currentContext()"));
      conBody.directStatement("if(context == null){");
      conBody.directStatement("  httpClient = io.vertx.core.Vertx.vertx().createHttpClient(options);");
      conBody.directStatement("}");
      conBody.directStatement("else{");
      conBody.directStatement("  httpClient = io.vertx.core.Vertx.currentContext().owner().createHttpClient(options);");
      conBody.directStatement("}");

      /* constructor, init the httpClient */
      JMethod consructor2 = jc.constructor(JMod.PUBLIC);
      consructor2.param(String.class, "host");
      consructor2.param(int.class, "port");
      consructor2.param(String.class, "tenantId");
      JBlock conBody2 = consructor2.body();
      conBody2.directStatement("this(host, port, tenantId, true);");

      /* constructor, init the httpClient */
      JMethod consructor3 = jc.constructor(JMod.PUBLIC);
      JBlock conBody3 = consructor3.body();
      conBody3.directStatement("this(\"localhost\", 8081, \"folio_demo\", false);");
      consructor3.javadoc().add("Convenience constructor for tests ONLY!<br>Connect to localhost on 8081 as folio_demo tenant.");

    } catch (Exception e) {
      e.printStackTrace();
    }

  }

  public void generateCloseClient(){
    JMethod jmCreate = jc.method(JMod.PUBLIC, void.class, "close");
    jmCreate.javadoc().add("Close the client. Closing will close down any "
        + "pooled connections. Clients should always be closed after use.");
    JBlock body = jmCreate.body();
    body.directStatement("httpClient.close();");

  }

  public void generateMethodMeta(String methodName, JsonObject params, String url,
      String httpVerb, JsonArray contentType, JsonArray accepts){

    /* Adding method to the Class which is public and returns void */

    String conciseName = massageMethodName(methodName);

    JMethod jmCreate = jc.method(JMod.PUBLIC, void.class, conciseName);
    JBlock body = jmCreate.body();

    /* create the query parameter string builder */
    body.directStatement("StringBuilder queryParams = new StringBuilder(\"?\");");


    ////////////////////////---- Handle place holders in the url  ----//////////////////
    /* create request */
    if(url == null){
      //if there is no path associated with a function
      //use the @path from the class
      url = globalPath;
    }
    else{
      /* Handle place holders in the URL
       * replace {varName} with "+varName+" so that it will be replaced
       * in the url at runtime with the correct values */
      Matcher m = Pattern.compile("\\{.*?\\}").matcher(url);
      while(m.find()){
        String varName = m.group().replace("{","").replace("}", "");
        url = url.replace("{"+varName+"}", "\"+"+varName+"+\"");
      }

      url = "\""+url.substring(1)+"\"+queryParams.toString()";
    }

    /* Adding java doc for method */
    jmCreate.javadoc().add("Service endpoint " + url);


    /* iterate on function params and add the relevant ones
     * --> functionSpecificQueryParamsPrimitives is populated by query parameters that are primitives
     * --> functionSpecificHeaderParams (used later on) is populated by header params
     * --> functionSpecificQueryParamsEnums is populated by query parameters that are enums */
    Iterator<Entry<String, Object>> paramList = params.iterator();

    boolean bodyContentExists[] = new boolean[]{false};
    paramList.forEachRemaining(entry -> {
      String valueName = ((JsonObject) entry.getValue()).getString("value");
      String valueType = ((JsonObject) entry.getValue()).getString("type");
      String paramType = ((JsonObject) entry.getValue()).getString("param_type");
      if(handleParams(jmCreate, paramType, valueType, valueName)){
        bodyContentExists[0] = true;
      }
    });

    //////////////////////////////////////////////////////////////////////////////////////

    /* create the http client request object */
    body.directStatement("io.vertx.core.http.HttpClientRequest request = httpClient."+
        httpVerb.substring(httpVerb.lastIndexOf(".")+1).toLowerCase()+"("+url+");");
    body.directStatement("request.handler(responseHandler);");

    /* add headers to request */
    functionSpecificHeaderParams.forEach( val -> {
      body.directStatement(val);
    });
    //reset for next method usage
    functionSpecificHeaderParams = new ArrayList<String>();

    /* add content and accept headers if relevant */
    if(contentType != null){
      String cType = contentType.toString().replace("\"", "").replace("[", "").replace("]", "");
      if(contentType.contains("multipart/form-data")){
        body.directStatement("request.putHeader(\"Content-type\", \""+cType+"; boundary=--BOUNDARY\");");
      }
      else{
        body.directStatement("request.putHeader(\"Content-type\", \""+cType+"\");");
      }
    }
    if(accepts != null){
      String aType = accepts.toString().replace("\"", "").replace("[", "").replace("]", "");
      body.directStatement("request.putHeader(\"Accept\", \""+aType+"\");");
    }

    /* push tenant id into x-okapi-tenant and authorization headers for now */
    body.directStatement("if(tenantId != null){");
    body.directStatement(" request.putHeader(\"Authorization\", tenantId);");
    body.directStatement(" request.putHeader(\""+OKAPI_HEADER_TENANT+"\", tenantId);");
    body.directStatement("}");
    /* add response handler to each function */
    JClass handler = jCodeModel.ref(Handler.class).narrow(HttpClientResponse.class);
    jmCreate.param(handler, "responseHandler");

    /* if we need to pass data in the body */
    if(bodyContentExists[0]){
      body.directStatement("request.putHeader(\"Content-Length\", buffer.length()+\"\");");
      body.directStatement("request.setChunked(true);");
      body.directStatement("request.write(buffer);");
    }

    body.directStatement("request.end();");

  }

  private void addParameter(JBlock methodBody, String valueName, Boolean encode, Boolean simple) {
    JBlock b = methodBody;
    if (!simple) {
      JConditional _if = methodBody._if(JExpr.ref(valueName).ne(JExpr._null()));
      b = _if._then();
    }
    b.invoke(JExpr.ref("queryParams"), "append").arg(JExpr.lit(valueName + "="));
    if (encode) {
      JTryBlock tb = b._try();
      JExpression expr = JExpr.direct("java.net.URLEncoder.encode(" + valueName + ", \"UTF-8\")");
      tb.body().invoke(JExpr.ref("queryParams"), "append").arg(expr);
      JClass jc1 = jCodeModel.ref(UnsupportedEncodingException.class);
      JCatchBlock cb2 = tb._catch(jc1);
      JVar e_var = cb2.param("e");
      cb2.body().invoke(JExpr.ref("e"), "printStackTrace");
    } else {
      b.invoke(JExpr.ref("queryParams"), "append").arg(JExpr.ref(valueName));
    }
    b.invoke(JExpr.ref("queryParams"), "append").arg(JExpr.lit("&"));
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

    JBlock methodBody = method.body();

    if (AnnotationGrabber.NON_ANNOTATED_PARAM.equals(paramType) /*&& !FILE_UPLOAD_PARAM.equals(valueType)*/) {
      try {
        // this will also validate the json against the pojo created from the schema
        Class<?> entityClazz = Class.forName(valueType);

        if (!valueType.equals("io.vertx.core.Handler") && !valueType.equals("io.vertx.core.Context") &&
            !valueType.equals("java.util.Map")) {

          /* this is a post or put since our only options here are receiving a reader (data in body) or
           * entity - which is also data in body - but we can only have one since a multi part body
           * should be indicated by a multipart objector input stream in the body */
          methodBody.directStatement( "io.vertx.core.buffer.Buffer buffer = io.vertx.core.buffer.Buffer.buffer();" );

          if("java.io.Reader".equals(valueType)){
            method.param(Reader.class, "reader");
            method._throws(Exception.class);
            methodBody.directStatement( "if(reader != null){buffer.appendString(org.apache.commons.io.IOUtils.toString(reader));}" );
          }
          else if("java.io.InputStream".equals(valueType)){
            method.param(InputStream.class, "inputStream");
            methodBody.directStatement( "java.io.ByteArrayOutputStream result = new java.io.ByteArrayOutputStream();");
            methodBody.directStatement( "byte[] buffer1 = new byte[1024];");
            methodBody.directStatement( "int length;\n");
            methodBody.directStatement( "while ((length = inputStream.read(buffer1)) != -1) {");
            methodBody.directStatement( "result.write(buffer1, 0, length);");
            methodBody.directStatement( "}");
            methodBody.directStatement( "buffer.appendBytes(result.toByteArray());");
            method._throws(IOException.class);
          }
          else if("javax.mail.internet.MimeMultipart".equals(valueType)){
            method.param(MimeMultipart.class, "mimeMultipart");
            method._throws(MessagingException.class);
            method._throws(IOException.class);
            methodBody.directStatement("if(mimeMultipart != null) {int parts = mimeMultipart.getCount();");
            methodBody.directStatement("StringBuilder sb = new StringBuilder();");
            methodBody.directStatement("for (int i = 0; i < parts; i++){");
            methodBody.directStatement("javax.mail.BodyPart bp = mimeMultipart.getBodyPart(i);");
            methodBody.directStatement("sb.append(\"----BOUNDARY\\r\\n\")");
            methodBody.directStatement(".append(\"Content-Disposition: \").append(bp.getDisposition()).append(\"; name=\\\"\").append(bp.getFileName())");
             //   + " {sb.append(mimeMultipart.getBodyPart(i).toString());}");
            methodBody.directStatement(".append(\"\\\"; filename=\\\"\").append(bp.getFileName()).append(\"\\\"\\r\\n\")");
            methodBody.directStatement(".append(\"Content-Type: application/octet-stream\\r\\n\")");
            methodBody.directStatement(".append(\"Content-Transfer-Encoding: binary\\r\\n\")");
            methodBody.directStatement(".append(\"\\r\\n\").append( bp.getContent() ).append(\"\\r\\n\\r\\n\");}");
            methodBody.directStatement("buffer.appendString(sb.append(\"----BOUNDARY\\r\\n\").toString());}");
          }
          else{
            if(mappingType.equals("postgres")){
              method._throws(Exception.class);
              methodBody.directStatement( "buffer.appendString("
                  + "org.folio.rest.persist.PostgresClient.pojo2json("+entityClazz.getSimpleName()+"));");
            }else{
              methodBody.directStatement( "buffer.appendString("
                  + "org.folio.rest.tools.utils.JsonUtils.entity2Json("+entityClazz.getSimpleName()+").encode());");
            }
            method.param(entityClazz, entityClazz.getSimpleName());
          }
          return true;
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
      functionSpecificHeaderParams.add("request.putHeader(\""+valueName+"\", "+valueName+");");
    }
    else if (AnnotationGrabber.QUERY_PARAM.equals(paramType)) {
      // support enum, numbers or strings as query parameters
      try {
        if (valueType.contains("String")) {
          method.param(String.class, valueName);
          addParameter(methodBody, valueName, true, false);
        } else if (valueType.contains("int")) {
          method.param(int.class, valueName);
          addParameter(methodBody, valueName, false, true);
        } else if (valueType.contains("boolean")) {
          method.param(boolean.class, valueName);
          addParameter(methodBody, valueName, false, true);
        } else if (valueType.contains("BigDecimal")) {
          method.param(BigDecimal.class, valueName);
          addParameter(methodBody, valueName, false, false);
        } else { // enum object type
          try {
            String enumClazz = replaceLast(valueType, ".", "$");
            Class<?> enumClazz1 = Class.forName(enumClazz);
            if (enumClazz1.isEnum()) {
              method.param(enumClazz1, valueName);
              addParameter(methodBody, valueName, false, false);
            }
          } catch (Exception ee) {
            ee.printStackTrace();
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return false;
  }

  public void generateClass(JsonObject classSpecificMapping) throws IOException{
    String genPath = System.getProperty("project.basedir") + PATH_TO_GENERATE_TO;
    jCodeModel.build(new File(genPath));
  }

  private static String replaceLast(String string, String substring, String replacement) {
    int index = string.lastIndexOf(substring);
    if (index == -1)
      return string;
    return string.substring(0, index) + replacement + string.substring(index + substring.length());
  }
}
