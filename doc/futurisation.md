# Futurisation in Vert.x 4

Vert.x 4 splits `Future` into `Future` and `Promise`.

`Promise` is the write side of an asynchronous result and
the `Future` its read side.

Creating and completing a `Future` has been deprecated in
Vert.x 3.8 and removed in 4.0. Use `Promise` and `promise.future()`
instead.

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
Future.<Void>future(promise -> fileSystem.createFile("/foo", promise)
).compose(v -> {
  return Future.<Void>future(promise -> fileSystem.writeFile("/foo", Buffer.buffer(), promise));
}).compose(v -> {
  return Future.<Void>future(promise -> fileSystem.move("/foo", "/bar", promise));
});
```

## Future as return value

Writing code for parallel or sequential composition of several
asynchronous methods is more easy if the methods don't take a
`Promise<T>` as parameter but return a `Future<T>`.

Using a Promise parameter:

```
void plusOne(int i, Promise<Integer> promise) {
  promise.complete(i + 1);
}
...
Future.future(promise -> plusOne(i, promise)
).compose(result -> {
  return Future.future(promise -> squared(result, promise));
}).compose(result -> {
  return Future.future(promise -> limit100(result, promise));
});
```

Using a Future as return value:

```
Future<Integer> plusOne(int i) {
  Promise<Integer> promise = Promise.promise();
  promise.complete(i + 1);
  return promise.future();
}
...
plusOne(i)
.compose(this::squared)
.compose(this::limit100);
```

## Types

Vert.x 3 `Future<T>` extends `AsyncResult<T>` _and_ `Handler<AsyncResult<T>>`.

Vert.x 4 `Future<T>` extends `AsyncResult<T>` _only_.

Vert.x 4 `Promise<T>` extends `Handler<AsyncResult<T>>` _only_.

To be prepared for Vert.x 4 use `Promise<T>` when a `Handler<AsyncResult<T>>`
is needed. Use `Future<T>` only if a `AsyncResult<T>` is needed.

References:
* https://vertx.io/blog/eclipse-vert-x-3-8-0-released/#future-api-improvements
* https://github.com/vert-x3/wiki/wiki/3.8.0-Deprecations-and-breaking-changes#future-creation-and-completion
* https://vertx.io/docs/vertx-core/java/#_sequential_composition
