package org.folio.rest.tools.utils;

import static io.vertx.core.http.HttpHeaders.CONTENT_TYPE;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.Handler;
import io.vertx.core.http.HttpClientResponse;

public class ResponseHandler {
  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public static Handler<HttpClientResponse> any(
    CompletableFuture<Response> completed) {

    return responseHandler(completed,
      responseToCheck -> true,
      failingResponse -> null);
  }

  public static Handler<HttpClientResponse> empty(
    CompletableFuture<Response> completed) {

    return response -> {
      try {
        int statusCode = response.statusCode();

        completed.complete(new Response(statusCode, null, null));
      }
      catch(Exception e) {
        completed.completeExceptionally(e);
      }
    };
  }

  public static Handler<HttpClientResponse> json(
    CompletableFuture<Response> completed) {

    return strictContentType(completed, "application/json");
  }

  public static Handler<HttpClientResponse> text(
    CompletableFuture<Response> completed) {

    return strictContentType(completed, "text/plain");
  }

  public static Handler<HttpClientResponse> jsonErrors(
    CompletableFuture<JsonErrorResponse> completed) {

    return response -> {
      response.bodyHandler(buffer -> {
        try {
          int statusCode = response.statusCode();
          String body = BufferHelper.stringFromBuffer(buffer);

          completed.complete(new JsonErrorResponse(statusCode, body,
            response.getHeader(CONTENT_TYPE)));

        } catch(Exception e) {
          completed.completeExceptionally(e);
        }
      });
    };
  }

  private static Handler<HttpClientResponse> strictContentType(
    CompletableFuture<Response> completed,
    String expectedContentType) {

    return responseHandler(completed,
      responseToCheck ->
        responseToCheck.getContentType().contains(expectedContentType),
      failingResponse -> new Exception(
        String.format("Expected Content Type: '%s' Actual Content Type: '%s' " +
            "(Status Code: '%s', Body: '%s')",
          expectedContentType, failingResponse.getContentType(),
          failingResponse.getStatusCode(), failingResponse.getBody())));
  }

  private static Handler<HttpClientResponse> responseHandler(
    CompletableFuture<Response> completed,
    Predicate<Response> expectation,
    Function<Response, Throwable> expectationFailed) {

    return vertxResponse -> {
        vertxResponse.bodyHandler(buffer -> {
          try {

            Response response = Response.from(vertxResponse, buffer);

            log.debug("Received Response: {}: {}", response.getStatusCode(), response.getContentType());
            log.debug("Received Response Body: {}", response.getBody());

            if(expectation.test(response)) {
              completed.complete(response);
            }
            else {
              completed.completeExceptionally(
                expectationFailed.apply(response));
            }
          } catch (Throwable e) {
            completed.completeExceptionally(e);
          }
        });
    };
  }

}
