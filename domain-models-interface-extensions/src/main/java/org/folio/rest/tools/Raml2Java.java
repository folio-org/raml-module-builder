package org.folio.rest.tools;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.folio.rest.annotations.Validate;
import org.folio.rest.tools.utils.Enum2Annotation;
import org.raml.jaxrs.codegen.core.ext.AbstractGeneratorExtension;
import org.raml.model.Action;
import org.raml.model.MimeType;

import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Table;
import com.sun.codemodel.JAnnotationUse;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JCommentPart;
import com.sun.codemodel.JDocComment;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JVar;

import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class Raml2Java extends AbstractGeneratorExtension {

  private static final Logger log = LoggerFactory.getLogger(Raml2Java.class);

  private static Table<String, String, JsonObject> overrideMap = HashBasedTable.create();
  private static boolean overrideFileExists = true;

  @Override
  public void onAddResourceMethod(JMethod method, Action action, MimeType bodyMimeType, Collection<MimeType> uniqueResponseMimeTypes) {

    super.onAddResourceMethod(method, action, bodyMimeType, uniqueResponseMimeTypes);
    try {
      //init map of annotations to override
      handleOverrides();
    } catch (IOException e1) {
      log.error(e1.getMessage(), e1);
    }

    generateOverrides(method, action);

    // jdoc the new parameters in all functions
    JDocComment methodDoc = method.javadoc();

    //document params
    JCommentPart paramDoc = methodDoc.addParam("asyncResultHandler");
    paramDoc.append("A <code>Handler<AsyncResult<Response>>></code> handler ");
    paramDoc.append("{@link " + Handler.class.getName() + "}");
    paramDoc
        .append(" which must be called as follows - Note the 'GetPatronsResponse' should be replaced with '[nameOfYourFunction]Response': (example only) <code>asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetPatronsResponse.withJsonOK( new ObjectMapper().readValue(reply.result().body().toString(), Patron.class))));</code> in the final callback (most internal callback) of the function.");

    paramDoc = methodDoc.addParam("vertxContext");
    paramDoc.append(" The Vertx Context Object <code>io.vertx.core.Context</code> ");

    //add routingContext param if indicated in generate runner plugin in pom
    //String endpoints2addRoutingContext = System.getProperty("generate_routing_context");
    String endpoints2addRoutingContext = PomReader.INSTANCE.getProps().getProperty("generate_routing_context");
    if(endpoints2addRoutingContext != null){
      String []rcFuncs = endpoints2addRoutingContext.split(",");
      for (int i = 0; i < rcFuncs.length; i++) {
        try {
          //System.out.println("endpoints2addRoutingContext = " + endpoints2addRoutingContext +
           // ", current path = " + action.getResource().getUri());
          if(rcFuncs[i].equalsIgnoreCase(action.getResource().getUri())){
            Class classRoutingContext = io.vertx.ext.web.RoutingContext.class;
            method.param(classRoutingContext, "routingContext");
            JCommentPart paramDoc1 = methodDoc.addParam("routingContext");
            paramDoc1.append("RoutingContext of the request. Note that the RMB framework handles all routing."
                + "This should only be used if a third party add-on to vertx needs the RC as input ");
          }
        } catch (Exception e) {
          log.error(e.getMessage(), e);
        }
      }
    }

    //add okapi headers to all interfaces generated from the raml
    String genericOKapiMap = "java.util.Map<String, String>";
    JClass genericOkapiMapRef = getCodeModel().ref(genericOKapiMap);
    method.param(genericOkapiMapRef, "okapiHeaders");

    // add parameter to all functions
    String genericTypeName = "io.vertx.core.Handler<io.vertx.core.AsyncResult<Response>>";
    JClass genericT = getCodeModel().ref(genericTypeName);
    method.param(genericT, "asyncResultHandler");

    Class classContext = io.vertx.core.Context.class;
    method.param(classContext, "vertxContext");

    // change return type to void for all functions
    method.type(getCodeModel().VOID);

    // annotate with validate class so that aspect picks it up for param
    // validation
    method.annotate(Validate.class);

  }

  private static void handleOverrides() throws IOException {
    String overrides = null;
    if(overrideFileExists){
      overrides = IOUtils.toString(Raml2Java.class.getClassLoader().getResourceAsStream(
        "overrides/raml_overrides.json"), "UTF-8");
      if(overrides == null){
        log.info("No overrides/raml_overrides.json file found, continuing...");
        overrideFileExists = false;
        return;
      }
    }
    else{
      return;
    }
    if(overrideMap.isEmpty()){
      try {
        JsonObject jobj = new JsonObject(overrides);
        JsonArray jar = jobj.getJsonArray("overrides");
        int size = jar.size();
        for (int i = 0; i < size; i++) {
          JsonObject overrideEntry = jar.getJsonObject(i);
          String type = overrideEntry.getString("type");
          String url = overrideEntry.getString("url");
          overrideMap.put(url, type, overrideEntry);
        }
      } catch (Exception e1) {
        log.error(e1.getMessage(), e1);
      }
    }
  }

  private void generateOverrides(JMethod method, Action action){
    //check if url has a param / params associated with it that needs overriding
    Map<String,JsonObject> overrideEntry = overrideMap.row(action.getResource().getUri());
    if(overrideEntry != null){
      //this endpoint was found in the config file and has an override associated with one of
      //its parameters
      JVar[] params = method.listParams();
      int []i = new int[]{0};
      for (i[0] = 0; i[0] < params.length; i[0]++) {
        Set<Entry<String, JsonObject>> entries = overrideEntry.entrySet();
        entries.forEach( entry -> {
          //iterate through the params of the generated function for this url + verb and look
          //for the parameter whose annotation we need to override
          JsonObject job = entry.getValue();
          String type = job.getString("type");
          Object value = job.getValue("value");
          String paramName = job.getString("paramName");
          if(!action.getResource().getUri().equalsIgnoreCase(job.getString("verb"))){
            //make sure the verb is aligned
            JAnnotationUse ann[] = new JAnnotationUse[]{null};
            if(paramName.equalsIgnoreCase(params[i[0]].name())){
              //we found the paramter that should be overridden
              boolean []found = new boolean[]{false};
              params[i[0]].annotations().forEach( use -> {
                //check if this annotation already exists for this paramter, if so it needs overriding
                String annotationType = Enum2Annotation.getAnnotation(type);
                if(annotationType != null && annotationType.endsWith(use.getAnnotationClass().name())){
                  found[0] = true;
                  ann[0] = use;
                }
              });
              if(!found[0]){
                //annotation not found for this paramter, add it
                JClass annClazz = new JCodeModel().ref(Enum2Annotation.getAnnotation(type));
                ann[0] = params[i[0]].annotate(annClazz);
              }
              setValueForAnnotation(ann[0], value, type);
            }
          }
        });
      }
    }
  }

  private void setValueForAnnotation(JAnnotationUse ann, Object value, String type){
    if(ann.getAnnotationClass().name().equals("Size")){
      //size can contain two values (min and max) so it is unlike the other potential annotations
      String []multipleAnnos = ((String)value).split(",");
      for (int j = 0; j < multipleAnnos.length; j++) {
        if(j == 0){
          ann.param("min", Integer.valueOf(multipleAnnos[j].trim()));
        }
        else{
          ann.param("max", Integer.valueOf(multipleAnnos[j].trim()));
        }
      }
    }
    else if(!type.equalsIgnoreCase("REQUIRED")){
      //a required annotation will add a @notnull annotation
      //THERE IS CURRENTLY NO SUPPORT TO MAKE A REQUIRED PARAM NOT REQUIRED
      if(value instanceof String){
        if(type.equalsIgnoreCase("PATTERN")){
          ann.param("regexp", (String)value);
        }
        else{
          ann.param("value", (String)value);
        }
      }
      else if(value instanceof Boolean){
        ann.param("value", (Boolean)value);
      }
      else if(value instanceof Integer){
        ann.param("value", (Integer)value);
      }
    }
  }
}
