package org.folio.rest.annotations;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicate if an implemented function wants posted / putted body content
 * streamed to it (meaning calling the function multiple times - once each time a
 * chunk of data is read from the http request)
 *
 */
@Target( { ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Stream {

}
