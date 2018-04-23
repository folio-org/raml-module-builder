package org.folio.rest.tools.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.ws.rs.core.Response;

import org.folio.rest.jaxrs.model.Error;
import org.folio.rest.jaxrs.model.Errors;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.persist.PgExceptionUtil;
import org.folio.rest.tools.RTFConsts;
import org.folio.rest.tools.messages.MessageConsts;
import org.folio.rest.tools.messages.Messages;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;


public class ValidationHelper {

  private static final Messages MESSAGES = Messages.getInstance();

  private static final String QUERYEXCEPTION = "QueryValidationException";

  public static Errors createValidationErrorMessage(String field, String value, String message){
    Errors e = new Errors();
    Error error = new Error();
    Parameter p = new Parameter();
    p.setKey(field);
    p.setValue(value);
    error.getParameters().add(p);
    error.setMessage(message);
    error.setCode("-1");
    error.setType(RTFConsts.VALIDATION_FIELD_ERROR);
    List<Error> l = new ArrayList<>();
    l.add(error);
    e.setErrors(l);
    return e;
  }

  public static void handleError(Throwable error, Handler<AsyncResult<Response>> asyncResultHandler){

    if(error != null){
      Response r = null;
      Map<Object,String> errorMessageMap = PgExceptionUtil.getBadRequestFields(error);
      if(errorMessageMap != null){
        //db error
        String desc = errorMessageMap.getOrDefault('D', "");
        String mess = errorMessageMap.getOrDefault('M', "");
        try{
          if(isInvalidUUID(mess)){
            int start = mess.indexOf("\"");
            int end = mess.indexOf("\"", start+1);
            String value = "";
            if(start != -1 && end != -1 && start < end){
              value = mess.substring(start+1, end);
            }
            r = withJsonUnprocessableEntity(ValidationHelper.createValidationErrorMessage("", value, mess));
          }
          else if(isDuplicate(mess) || isFKViolation(mess)){
            String[] errorDesc = desc.split("=");
            String field = errorDesc[0].substring(errorDesc[0].indexOf("(")+1,errorDesc[0].indexOf(")"));
            String value = errorDesc[1].substring(errorDesc[1].indexOf("(")+1,errorDesc[1].indexOf(")"));
            r = withJsonUnprocessableEntity(ValidationHelper.createValidationErrorMessage(field, value, mess));
          }
          else if(isAuthFailed(mess)){
            r = withForbiddenEntity();
          }
          else{
            r = withPlainInternalServerError("");
          }
        }
        catch(Exception e){
          r = withPlainInternalServerError("");
        }
      }
      else{
        //not a db error
        r = handleNonDBError(error);
      }
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(r));
    }
    else {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(withPlainInternalServerError(
        MESSAGES.getMessage("en", MessageConsts.InternalServerError))));
    }
  }

  private static Response handleNonDBError(Throwable error){
    if(error.getCause() != null){
      if(error.getCause().getClass().getSimpleName().endsWith("ParseException")){
        return
          withJsonUnprocessableEntity(ValidationHelper.createValidationErrorMessage("CQL parse error", "",
            error.getLocalizedMessage()));
      }
      else if(error.getCause().getClass().getSimpleName().endsWith(QUERYEXCEPTION)){
        String field = error.getMessage();
        try {
          int start = error.getMessage().indexOf("'");
          int end = error.getMessage().lastIndexOf("'");
          if(start != -1 && end != -1){
            field = field.substring(start+1, end);
          }
          if(field.contains(QUERYEXCEPTION)){
            field = field.substring(field.indexOf(QUERYEXCEPTION)+QUERYEXCEPTION.length()+1);
          }
        } catch (Exception e1) {
          e1.printStackTrace();
        }
        Errors e = ValidationHelper.createValidationErrorMessage(field, "", error.getMessage());
        return withJsonUnprocessableEntity(e);
      }
      else{
        return withPlainInternalServerError(
          MESSAGES.getMessage("en", MessageConsts.InternalServerError));
      }
    }
    else{
      return withPlainInternalServerError(
        MESSAGES.getMessage("en", MessageConsts.InternalServerError));
    }
  }

  private static Response withJsonUnprocessableEntity(Errors entity) {
    Response.ResponseBuilder responseBuilder = Response.status(422).header("Content-Type", "application/json");
    responseBuilder.entity(entity);
    return responseBuilder.build();
  }

  private static Response withPlainInternalServerError(String entity) {
    Response.ResponseBuilder responseBuilder = Response.status(500).header("Content-Type", "text/plain");
    responseBuilder.entity(entity);
    return responseBuilder.build();
  }

  private static Response withForbiddenEntity() {
    Response.ResponseBuilder responseBuilder = Response.status(401).header("Content-Type", "text/plain");
    responseBuilder.entity("Unauthorized");
    return responseBuilder.build();
  }

  public static boolean isInvalidUUID(String errorMessage){
    if(errorMessage != null &&
        (errorMessage.contains("invalid input syntax for type uuid") /*postgres v10*/ ||
            errorMessage.contains("invalid input syntax for uuid") /*postgres v9.6*/)){
      return true;
    }
    else{
      return false;
    }
  }

  public static boolean isFKViolation(String errorMessage){
    if(errorMessage != null &&
        (errorMessage.contains("violates foreign key constraint"))){
      return true;
    }
    else{
      return false;
    }
  }

  public static boolean isDuplicate(String errorMessage){
    if(errorMessage != null && errorMessage.contains("duplicate key value violates unique constraint")){
      return true;
    }
    return false;
  }

  public static boolean isAuthFailed(String errorMessage){
    if(errorMessage != null && errorMessage.contains("password authentication failed for user")){
      return true;
    }
    return false;
  }
}
