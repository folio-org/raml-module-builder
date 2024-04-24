package org.folio.rest.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import org.folio.rest.validators.ElementsNotNullValidator;

@Constraint(validatedBy = { ElementsNotNullValidator.class })
@Target({ ElementType.FIELD, ElementType.PARAMETER })
@Retention(value = RetentionPolicy.RUNTIME)
@Documented
public @interface ElementsNotNull {
  String message() default "list can not contain null";

  Class<?>[] groups() default {};

  Class<? extends Payload>[] payload() default {};
}
