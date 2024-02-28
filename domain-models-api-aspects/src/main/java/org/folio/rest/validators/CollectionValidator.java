package org.folio.rest.validators;

import java.lang.annotation.Annotation;
import java.util.Collection;

import jakarta.validation.ConstraintValidator;

public interface CollectionValidator<A extends Annotation, E, C extends Collection<E>>
    extends ConstraintValidator<A, C> {

  public boolean isElementValid(E element);

}
