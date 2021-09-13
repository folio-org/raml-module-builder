package org.folio.rest.tools.utils;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import org.folio.rest.jaxrs.model.Error;
import org.folio.rest.jaxrs.model.Errors;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.persist.PgExceptionUtil;
import org.folio.rest.resource.DomainModelConsts;
import org.folio.rest.tools.messages.MessageConsts;
import org.folio.rest.tools.messages.Messages;

import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ValidationHelper {
  private static final Messages MESSAGES = Messages.getInstance();
  private static final String QUERYEXCEPTION = "QueryValidationException";
  private static final String CONTENT_TYPE = "Content-Type";

  public static Errors createValidationErrorMessage(String field, String value, String message) {
    Errors e = new Errors();
    Error error = new Error();
    Parameter p = new Parameter();
    p.setKey(field);
    p.setValue(value);
    error.getParameters().add(p);
    error.setMessage(message);
    error.setCode("-1");
    error.setType(DomainModelConsts.VALIDATION_FIELD_ERROR);
    List<Error> l = new ArrayList<>();
    l.add(error);
    e.setErrors(l);
    return e;
  }

  public static void handleError(Throwable error, Handler<AsyncResult<Response>> asyncResultHandler) {
    if (error != null) {
      Response r = null;
      Map<Character, String> errorMessageMap = PgExceptionUtil.getBadRequestFields(error);
      if (errorMessageMap != null) {
        //db error
        String desc = errorMessageMap.getOrDefault('D', "");
        String mess = errorMessageMap.getOrDefault('M', "");
        try {
          if (isInvalidUUID(mess)) {
            int start = mess.indexOf('\"');
            int end = mess.indexOf('\"', start + 1);
            String UUIDvalue = "";
            if (start != -1 && end != -1 && start < end) {
              UUIDvalue = mess.substring(start + 1, end);
            }
            r = withJsonUnprocessableEntity(ValidationHelper.createValidationErrorMessage("", UUIDvalue, mess));
          } else if (isDuplicate(mess) || isFKViolation(mess)) {
            String[] errorDesc = desc.split("=");
            String field = errorDesc[0].substring(errorDesc[0].indexOf('(') + 1, errorDesc[0].indexOf(')'));
            String value = errorDesc[1].substring(errorDesc[1].indexOf('(') + 1, errorDesc[1].indexOf(')'));
            if (isDuplicate(mess))
              mess = "duplicate " + field + " value violates unique constraint :" + value;
            r = withJsonUnprocessableEntity(ValidationHelper.createValidationErrorMessage(field, value, mess));
          } else if (isAuthFailed(mess)) {
            r = withForbiddenEntity();
          } else {
            r = withPlainInternalServerError("");
          }
        } catch (Exception e) {
          r = withPlainInternalServerError("");
        }
      } else {
        //not a db error
        r = handleNonDBError(error);
      }
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(r));
    } else {
      asyncResultHandler.handle(io.vertx.core.Future.succeededFuture(withPlainInternalServerError(
          MESSAGES.getMessage("en", MessageConsts.InternalServerError))));
    }
  }

  private static Response handleNonDBError(Throwable error) {
    if (error.getCause() != null) {
      if (error.getCause().getClass().getSimpleName().endsWith("ParseException")) {
        return
            withJsonUnprocessableEntity(ValidationHelper.createValidationErrorMessage("CQL parse error", "",
                error.getLocalizedMessage()));
      } else if (error.getCause().getClass().getSimpleName().endsWith(QUERYEXCEPTION)) {
        String message = error.getCause().getMessage();
        String field = "";
        if (message != null) {
          int start = message.indexOf('\'');
          int end = message.lastIndexOf('\'');
          if (start != -1 && end != -1) {
            field = message.substring(start + 1, end);
          }
        } else {
          message = "";
        }
        Errors e = ValidationHelper.createValidationErrorMessage(field, "", message);
        return withJsonUnprocessableEntity(e);
      } else {
        return withPlainInternalServerError(
            MESSAGES.getMessage("en", MessageConsts.InternalServerError));
      }
    } else {
      return withPlainInternalServerError(
          MESSAGES.getMessage("en", MessageConsts.InternalServerError));
    }
  }

  private static Response withJsonUnprocessableEntity(Errors entity) {
    Response.ResponseBuilder responseBuilder = Response.status(422).header(CONTENT_TYPE, "application/json");
    responseBuilder.entity(entity);
    return responseBuilder.build();
  }

  private static Response withPlainInternalServerError(String entity) {
    Response.ResponseBuilder responseBuilder = Response.status(500).header(CONTENT_TYPE, "text/plain");
    responseBuilder.entity(entity);
    return responseBuilder.build();
  }

  private static Response withForbiddenEntity() {
    Response.ResponseBuilder responseBuilder = Response.status(401).header(CONTENT_TYPE, "text/plain");
    responseBuilder.entity("Unauthorized");
    return responseBuilder.build();
  }

  public static boolean isInvalidUUID(String errorMessage) {
    return (errorMessage != null &&
        (errorMessage.contains("invalid input syntax for type uuid") /*postgres v10*/ ||
            errorMessage.contains("invalid input syntax for uuid") /*postgres v9.6*/));
  }

  public static boolean isFKViolation(String errorMessage) {
    return (errorMessage != null &&
        (errorMessage.contains("violates foreign key constraint")));
  }

  public static boolean isDuplicate(String errorMessage) {
    return (errorMessage != null && errorMessage.contains("duplicate key value violates unique constraint"));

  }

  public static boolean isAuthFailed(String errorMessage) {
    return (errorMessage != null && errorMessage.contains("password authentication failed for user"));
  }
}
