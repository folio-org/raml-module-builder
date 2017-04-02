package org.folio.rest.tools.utils;

import java.util.ArrayList;
import java.util.List;

import org.folio.rest.jaxrs.model.Error;
import org.folio.rest.jaxrs.model.Errors;
import org.folio.rest.jaxrs.model.Parameter;
import org.folio.rest.tools.RTFConsts;


public class ValidationHelper {

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
}
