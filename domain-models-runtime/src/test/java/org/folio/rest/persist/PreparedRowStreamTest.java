package org.folio.rest.persist;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

import io.vertx.core.Handler;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.STRICT_STUBS)
class PreparedRowStreamTest {
  @Mock
  PreparedStatement mockedPreparedStatement;
  @Mock
  RowStream<Row> mockedRowStream;

  @Test
  void fetch() {
    PreparedRowStream preparedRowStream = new PreparedRowStream(mockedRowStream);
    preparedRowStream.fetch(234);
    verify(mockedRowStream).fetch(234);
  }

  @Test
  void pauseResume() {
    PreparedRowStream preparedRowStream = new PreparedRowStream(mockedRowStream);
    preparedRowStream.pause();
    verify(mockedRowStream, times(1)).pause();
    verify(mockedRowStream, never()).resume();
    preparedRowStream.resume();
    verify(mockedRowStream, times(1)).pause();
    verify(mockedRowStream, times(1)).resume();
  }

  @Test
  void exceptionHandlerOneFailure() {
    AtomicReference<Handler<Throwable>> exceptionHandler = new AtomicReference<>();
    when(mockedRowStream.exceptionHandler(any())).thenAnswer(i -> {
      exceptionHandler.set(i.getArgument(0));
      return mockedRowStream;
    });
    PreparedRowStream preparedRowStream = new PreparedRowStream(mockedRowStream);
    exceptionHandler.get().handle(new RuntimeException("foo"));
    assertThat(preparedRowStream.getResult().failed(), is(true));
    assertThat(preparedRowStream.getResult().cause().getMessage(), is("foo"));
  }

  @Test
  void exceptionHandlerTwoFailures() {
    AtomicReference<Handler<Throwable>> exceptionHandler = new AtomicReference<>();
    when(mockedRowStream.exceptionHandler(any())).thenAnswer(i -> {
      exceptionHandler.set(i.getArgument(0));
      return mockedRowStream;
    });
    PreparedRowStream preparedRowStream = new PreparedRowStream(mockedRowStream);
    exceptionHandler.get().handle(new RuntimeException("bar"));
    exceptionHandler.get().handle(new RuntimeException("baz"));
    assertThat(preparedRowStream.getResult().failed(), is(true));
    assertThat(preparedRowStream.getResult().cause().getMessage(), is("bar"));
  }

  @Test
  void exceptionAfterEnd() {
    AtomicReference<Handler<Void>> endHandler = new AtomicReference<>();
    when(mockedRowStream.endHandler(any())).thenAnswer(i -> {
      endHandler.set(i.getArgument(0));
      return mockedRowStream;
    });
    AtomicReference<Handler<Throwable>> exceptionHandler = new AtomicReference<>();
    when(mockedRowStream.exceptionHandler(any())).thenAnswer(i -> {
      exceptionHandler.set(i.getArgument(0));
      return mockedRowStream;
    });
    PreparedRowStream preparedRowStream = new PreparedRowStream(mockedRowStream);
    endHandler.get().handle(null);
    exceptionHandler.get().handle(new RuntimeException("foo"));
    assertThat(preparedRowStream.getResult().succeeded(), is(true));
  }
}
