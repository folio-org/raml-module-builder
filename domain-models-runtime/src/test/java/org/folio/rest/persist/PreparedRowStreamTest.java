package org.folio.rest.persist;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.*;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.PreparedStatement;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.AdditionalAnswers;
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
  void closeHandlerOneFailure() {
    doAnswer(AdditionalAnswers.answerVoid((Handler<AsyncResult<Void>> handler)
        -> handler.handle(Future.succeededFuture())))
        .when(mockedPreparedStatement).close(any());
    doAnswer(AdditionalAnswers.answerVoid((Handler<AsyncResult<Void>> handler)
        -> handler.handle(Future.failedFuture("fail"))))
        .when(mockedRowStream).close(any());
    PreparedRowStream preparedRowStream = new PreparedRowStream(mockedRowStream);
    StringBuilder s = new StringBuilder();
    preparedRowStream.close(handler -> s.append(handler.cause().getMessage()));
    assertThat(s.toString(), is("fail"));
    verify(mockedPreparedStatement).close(any());
  }

  @Test
  void closeHandlerTwoFailures() {
    doAnswer(AdditionalAnswers.answerVoid((Handler<AsyncResult<Void>> handler)
        -> handler.handle(Future.failedFuture("fail1"))))
        .when(mockedPreparedStatement).close(any());
    doAnswer(AdditionalAnswers.answerVoid((Handler<AsyncResult<Void>> handler)
        -> handler.handle(Future.failedFuture("fail2"))))
        .when(mockedRowStream).close(any());
    PreparedRowStream preparedRowStream = new PreparedRowStream(mockedRowStream);
    StringBuilder s = new StringBuilder();
    preparedRowStream.close(handler -> s.append(handler.cause().getMessage()));
    assertThat(s.toString(), is("fail2"));
    verify(mockedPreparedStatement).close(any());
  }
}
