package org.folio.rest.tools;

import io.vertx.core.Handler;

import java.util.Collection;
import org.raml.jaxrs.codegen.core.ext.AbstractGeneratorExtension;
import org.raml.model.Action;
import org.raml.model.MimeType;
import org.folio.rest.annotations.Validate;
import com.sun.codemodel.JCommentPart;
import com.sun.codemodel.JDocComment;
import com.sun.codemodel.JMethod;
import com.sun.codemodel.JClass;

public class Raml2Java extends AbstractGeneratorExtension {

  @Override
  public void onAddResourceMethod(JMethod method, Action action, MimeType bodyMimeType, Collection<MimeType> uniqueResponseMimeTypes) {

    super.onAddResourceMethod(method, action, bodyMimeType, uniqueResponseMimeTypes);

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

    // jdoc the new parameters in all functions
    JDocComment methodDoc = method.javadoc();
    JCommentPart paramDoc = methodDoc.addParam("asyncResultHandler");
    paramDoc.append("A <code>Handler<AsyncResult<Response>>></code> handler ");
    paramDoc.append("{@link " + Handler.class.getName() + "}");
    paramDoc
        .append(" which must be called as follows - Note the 'GetPatronsResponse' should be replaced with '[nameOfYourFunction]Response': (example only) <code>asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(GetPatronsResponse.withJsonOK( new ObjectMapper().readValue(reply.result().body().toString(), Patron.class))));</code> in the final callback (most internal callback) of the function.");

    paramDoc = methodDoc.addParam("vertxContext");
    paramDoc.append(" The Vertx Context Object <code>io.vertx.core.Context</code> ");

  }

}
