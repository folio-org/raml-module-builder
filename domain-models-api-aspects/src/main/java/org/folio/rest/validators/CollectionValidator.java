package org.folio.rest.validators;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Optional;

import javax.validation.ConstraintValidator;

public interface CollectionValidator<A extends Annotation, E, C extends Collection<E>>
    extends ConstraintValidator<A, C> {

  public boolean isEntityValid(Optional<E> element);

}
