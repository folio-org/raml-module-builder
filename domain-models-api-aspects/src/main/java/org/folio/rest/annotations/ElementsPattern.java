package org.folio.rest.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import jakarta.validation.Constraint;
import jakarta.validation.Payload;

import org.folio.rest.validators.ElementsPatternValidator;

@Constraint(validatedBy = { ElementsPatternValidator.class })
@Target({ ElementType.FIELD, ElementType.PARAMETER })
@Retention(value = RetentionPolicy.RUNTIME)
@Documented
public @interface ElementsPattern {
  String regexp();

  String message() default "elements in list must match pattern";

  Class<?>[] groups() default {};

  Class<? extends Payload>[] payload() default {};
}
