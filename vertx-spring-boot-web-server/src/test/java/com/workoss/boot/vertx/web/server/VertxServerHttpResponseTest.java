package com.workoss.boot.vertx.web.server;

import io.vertx.core.http.HttpServerResponse;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class VertxServerHttpResponseTest {

    @Test
    void reportsCommittedOnceVertxWroteTheResponseHead() {
        HttpServerResponse nativeResponse = mock(HttpServerResponse.class);
        VertxServerHttpResponse response = new VertxServerHttpResponse(nativeResponse);

        // A WebSocket upgrade writes the 101 straight through Vert.x and never calls commit().
        assertThat(response.isCommitted()).isFalse();

        when(nativeResponse.headWritten()).thenReturn(true);
        assertThat(response.isCommitted()).isTrue();
    }
}
