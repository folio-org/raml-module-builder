package org.folio.rest.tools;

import java.util.Collection;

import org.folio.rest.annotations.Validate;
import org.raml.jaxrs.codegen.core.ext.AbstractGeneratorExtension;
import org.raml.model.Action;
import org.raml.model.MimeType;

import com.sun.codemodel.JClass;
import com.sun.codemodel.JCommentPart;
import com.sun.codemodel.JDocComment;
import com.sun.codemodel.JMethod;

import io.vertx.core.Handler;

public class Raml2Java extends AbstractGeneratorExtension {

  @Override
  public void onAddResourceMethod(JMethod method, Action action, MimeType bodyMimeType, Collection<MimeType> uniqueResponseMimeTypes) {

    super.onAddResourceMethod(method, action, bodyMimeType, uniqueResponseMimeTypes);


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
          e.printStackTrace();
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

}
