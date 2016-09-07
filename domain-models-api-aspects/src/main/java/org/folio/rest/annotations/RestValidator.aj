package org.folio.rest.annotations;

import java.util.Arrays;
import java.util.Set;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.ValidationException;
import javax.validation.ValidatorFactory;
import javax.validation.executable.ExecutableValidator;
import org.aspectj.lang.reflect.MethodSignature;

public aspect RestValidator {

  static private ValidatorFactory factory;

  static {
    factory = Validation.buildDefaultValidatorFactory();
  }

  static private ExecutableValidator getMethodValidator() {
    return factory.getValidator().forExecutables();
  }

  pointcut validatedMethodCall() : execution(@Validate * *(..));

  /**
   * Validates the method parameters.
   */
  before() : validatedMethodCall() {

    MethodSignature methodSignature = (MethodSignature) thisJoinPoint.getSignature();

    System.out.println("Validating call: with args " + methodSignature.getMethod() + Arrays.toString(thisJoinPoint.getArgs()));

    String[] params = methodSignature.getParameterNames();

    Set<? extends ConstraintViolation<?>> validationErrors = getMethodValidator().validateParameters(thisJoinPoint.getThis(),
        methodSignature.getMethod(), thisJoinPoint.getArgs());

    if (validationErrors.isEmpty()) {
      System.out.println("Valid call......");
    } else {
      System.out.println("Invalid call......");
      RuntimeException ex = buildValidationException(validationErrors, params);
      throw ex;
    }
  }

  /**
   * @param validationErrors
   *          The errors detected in a method/constructor call.
   * @return A RuntimeException with information about the detected validation
   *         errors.
   */
  private RuntimeException buildValidationException(Set<? extends ConstraintViolation<?>> validationErrors, String[] paramNames) {
    StringBuilder sb = new StringBuilder();
    for (ConstraintViolation<?> cv : validationErrors) {
      // sb.append("\n" + cv.getPropertyPath() + "{" + cv.getInvalidValue() +
      // "} : " + cv.getMessage());
      String positionOfBadParam = cv.getPropertyPath().toString();
      if (positionOfBadParam != null) {
        try {
          String paramName = paramNames[Integer.valueOf(positionOfBadParam.substring(positionOfBadParam.length() - 1,
              positionOfBadParam.length()))];
          sb.append("\n '" + paramName + "' parameter is incorrect. parameter value {" + cv.getInvalidValue() + "} is not valid: "
              + cv.getMessage());

        } catch (Exception e) {
          sb.append("\n not all parameters were passed properly. parameter value {" + cv.getInvalidValue() + "} is not valid: "
              + cv.getMessage());

        }
      }
    }
    return new ValidationException(sb.toString());
  }
}
