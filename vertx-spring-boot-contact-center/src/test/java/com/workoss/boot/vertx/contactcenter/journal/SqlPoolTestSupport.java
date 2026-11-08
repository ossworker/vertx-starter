package com.workoss.boot.vertx.contactcenter.journal;

import io.vertx.core.Future;
import io.vertx.sqlclient.DatabaseException;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowIterator;
import io.vertx.sqlclient.RowSet;

import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

public final class SqlPoolTestSupport {
    private SqlPoolTestSupport() {
    }

    @SuppressWarnings("unchecked")
    public static RowSet<Row> rows(int affected, Row... values) {
        // A default Answer avoids nested Mockito stubbing when rows(...) is a thenReturn argument.
        return mock(RowSet.class, invocation -> switch (invocation.getMethod().getName()) {
            case "rowCount" -> affected;
            case "size" -> values.length;
            case "iterator" -> new RowIterator<Row>() {
                private final Iterator<Row> iterator = Arrays.asList(values).iterator();
                public boolean hasNext() { return iterator.hasNext(); }
                public Row next() { return iterator.next(); }
            };
            default -> org.mockito.Answers.RETURNS_DEFAULTS.answer(invocation);
        });
    }

    public static DatabaseException databaseFailure(boolean postgres, boolean duplicate) {
        return new DatabaseException("test database failure", postgres ? 0 : duplicate ? 1062 : 1045,
                duplicate ? postgres ? "23505" : "23000" : "08006", null) { };
    }

    public static <T> T await(Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    public static Throwable failure(Future<?> future) {
        return assertThrows(ExecutionException.class,
                () -> future.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS)).getCause();
    }
}
