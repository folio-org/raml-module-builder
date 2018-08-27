package org.folio.rest.validators.mapper;

import javax.validation.ValidationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.log4j.Logger;

@Provider
public class CustomValidationExceptionMapper implements ExceptionMapper<ValidationException> {

  private static final Logger log = Logger.getLogger(CustomValidationExceptionMapper.class);

  @Override
  public Response toResponse(ValidationException exception) {
    log.info("Handling validation exception: " + exception.getMessage());
    log.error("Validation Exception", exception.getCause());
    return Response.status(Response.Status.BAD_REQUEST).entity(exception.getMessage()).build();
  }

}
