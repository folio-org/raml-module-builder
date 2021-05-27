# Futurisation in Vert.x 4

Vert.x 4 splits `Future` into `Future` and `Promise`.

`Promise` is the write side of an asynchronous result and
the `Future` its read side.

Vert.x 3 `Future<T>` extends `AsyncResult<T>` _and_ `Handler<AsyncResult<T>>`.

Vert.x 4 `Future<T>` extends `AsyncResult<T>` _only_.

Vert.x 4 `Promise<T>` extends `Handler<AsyncResult<T>>` _only_.

To be prepared for Vert.x 4 use `Promise<T>` when a `Handler<AsyncResult<T>>`
is needed. Use `Future<T>` only if a `AsyncResult<T>` is needed.

Creating and completing a `Future` has been deprecated in
Vert.x 3.8 and removed in 4.0. Use `Promise`, `promise.future()`
or `Future.future(promise -> …)` instead.

Old code:

```
public Future<String> hello() {
  Future<String> future = Future.future();
  vertx.setTimer(1000, id -> future.complete("hello"));
  return future;
}
```

New code:

```
public Future<String> hello() {
  Promise<String> promise = Promise.promise();
  vertx.setTimer(1000, id -> promise.complete("hello"));
  return promise.future();
}
```

New code with concise `Future.future(promise -> …)`:

```
public Future<String> hello() {
  return Future.future(promise -> vertx.setTimer(1000, id -> promise.complete("hello")));
}
```

Use `Future.<T>future(promise -> …)` if the compiler cannot detect
the generic type:

```
public Future<Void> swallowInteger() {
  return Future.<Integer>future(promise -> plusOne(2, promise))
      .mapEmpty();
}
```

## Sequential composition

Example for sequential composition with `Future.compose`.

Old code:

```
FileSystem fileSystem = vertx.fileSystem();
Future<Void> future1 = Future.future();
fileSystem.createFile("/foo", future1);
future1.compose(v -> {
  Future<Void> future2 = Future.future();
  fileSystem.writeFile("/foo", Buffer.buffer(), future2);
  return future2;
}).compose(v -> {
  Future<Void> future3 = Future.future();
  fileSystem.move("/foo", "/bar", future3);
  return future3;
});
```

New code:

```
FileSystem fileSystem = vertx.fileSystem();
Future.<Void>future(promise -> fileSystem.createFile("/foo", promise))
.compose(v -> Future.<Void>future(promise -> fileSystem.writeFile("/foo", Buffer.buffer(), promise)))
.compose(v -> Future.<Void>future(promise -> fileSystem.move("/foo", "/bar", promise)));
```

## Future as return value

Writing code for parallel or sequential composition of several
asynchronous methods is more easy if the methods don't take a
`Promise<T>` as parameter but return a `Future<T>`.

Using a `Promise` parameter:

```
void plusOne(int i, Promise<Integer> promise) {
  promise.complete(i + 1);
}
...
Future.<Integer>future(promise -> plusOne(i, promise))
.compose(result -> Future.<Integer>future(promise -> squared(result, promise)))
.compose(result -> Future.<Integer>future(promise -> limit100(result, promise)));
```

Using a `Future` as return value:

```
Future<Integer> plusOne(int i) {
  return Future.succeededFuture(i + 1);
}
...
return plusOne(i)
.compose(this::squared)
.compose(this::limit100);
```

Adding a `Future` returning method is easy if a `Promise` argument
method already exists:

```
void squared(int i, Promise<Integer> promise) {
  final int MAX = (int) Math.sqrt(Integer.MAX_VALUE);
  if (Math.abs(i) > MAX) {
    promise.fail("expecting a value not exceeding " + MAX + " but got " + i);
    return;
  }
  promise.complete(i * i);
}

Future<Integer> squared(int i) {
  return Future.future(promise -> squared(i, promise));
}
```

However, the preferred way is to rewrite the method to return a `Future` and
call it from the `Promise` argument method. This avoids creating yet another
`Promise`:

```
Future<Integer> squared(int i) {
  final int MAX = (int) Math.sqrt(Integer.MAX_VALUE);
  if (Math.abs(i) > MAX) {
    return Future.failedFuture("expecting a value not exceeding " + MAX + " but got " + i);
  }
  return Future.succeededFuture(i * i);
}

void squared(int i, Promise<Integer> promise) {
  squared(i).onComplete(promise);
}
```

## HttpClient composition

The
[Vert.x documentation](https://vertx-web-site.github.io/docs/vertx-core/java/#_request_and_response_composition)
warns:

The HttpClient API "intentionally does not return a
`Future<HttpClientResponse>` because setting a completion handler on
the future can be racy when this is set outside of the event-loop",
"the API is event driven and you need to understand it otherwise you
might experience possible data races (i.e loosing events leading to
corrupted data)."

"[Vert.x Web Client](https://vertx-web-site.github.io/docs/vertx-web-client/java/)
is a higher level API alternative (in fact it is built on top of this
client) you might consider if this client is too low level for your
use cases".

## Result and error handling

Use `onSuccess` and `onFailure` to process the result or the failure:

```
return plusOne(i)
.compose(this::squared)
.compose(this::limit100)
.onSuccess(result -> log("Calculation result: " + result))
.onFailure(e -> log("Calculation failed: " + e.getMessage(), e));
```

This is more concise than using `onComplete`, `if`, `result()` and `cause()`:


```
return plusOne(i)
.compose(this::squared)
.compose(this::limit100)
.onComplete(ar -> {
  if (ar.succeeded()) {
    log("Calculation result: " + ar.result());
  } else {
    log("Calculation failed: " + ar.cause().getMessage(), ar.cause());
  }
});
```

`Future` has built-in error handling:
* `Future` catches all exceptions thrown in the `compose`, `flatMap`, `map`,
  `otherwise` and `recover` functions and propagates them by returning a failed `Future`.
* If a Future has failed it skips `compose`, `flatMap` and `map`.

This frees developers from most error handling code.

## References
* https://vertx.io/blog/eclipse-vert-x-3-8-0-released/#future-api-improvements
* https://github.com/vert-x3/wiki/wiki/3.8.0-Deprecations-and-breaking-changes#future-creation-and-completion
* https://vertx.io/docs/vertx-core/java/#_sequential_composition
