
package org.folio.rest.jaxrs.resource;

import java.math.BigDecimal;
import java.util.Date;
import java.util.List;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import io.vertx.core.Context;
import org.folio.rest.annotations.Validate;

@Path("unittests")
public interface TestResource {
    @GET
    @Path("books")
    @Produces({
        "application/json"
    })
    @Validate
    void getRmbtestsBooks(
        @QueryParam("author")
        @NotNull
        String author,
        @QueryParam("publicationDate")
        @NotNull
        Date publicationDate,
        @QueryParam("rating")
        BigDecimal rating,
        @QueryParam("isbn")
        @Size(min = 10)
        String isbn,
        @QueryParam("facets")
        List<String> facets, java.util.Map<String, String>okapiHeaders, io.vertx.core.Handler<io.vertx.core.AsyncResult<Response>>asyncResultHandler, Context vertxContext)
        throws Exception
    ;

    @GET
    @Produces({
        "application/json"
    })
    @Validate
    void getRmbtests(
        @QueryParam("name")
        @NotNull
        String name,
        @QueryParam("success")
        @NotNull
        Boolean success, java.util.Map<String, String>okapiHeaders, io.vertx.core.Handler<io.vertx.core.AsyncResult<Response>>asyncResultHandler, Context vertxContext)
        throws Exception
    ;
}
