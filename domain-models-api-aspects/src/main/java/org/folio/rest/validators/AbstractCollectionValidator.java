package org.folio.rest.validators;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Iterator;

import jakarta.validation.ConstraintValidatorContext;

public abstract class AbstractCollectionValidator<A extends Annotation, E, C extends Collection<E>>
    implements CollectionValidator<A, E, C> {

  @Override
  public boolean isValid(final C collection, final ConstraintValidatorContext context) {
    if (collection != null && !collection.isEmpty()) {
      Iterator<E> iterator = collection.iterator();
      while (iterator.hasNext()) {
        E element = iterator.next();
        if (element == null || !isElementValid(element)) {
          return false;
        }
      }
    }
    return true;
  }

}
