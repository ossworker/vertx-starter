package com.workoss.boot.vertx.contact;

import com.workoss.boot.vertx.contactcenter.ContactCenterWebSocketHandler;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileProps;
import io.vertx.core.file.FileSystem;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;
import reactor.core.publisher.Flux;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class UploadControllerTest {
    private FileSystem files;
    private ContactCenterWebSocketHandler authentication;
    private UploadController controller;

    @BeforeEach
    void setup() {
        Vertx vertx = mock(Vertx.class);
        files = mock(FileSystem.class);
        authentication = mock(ContactCenterWebSocketHandler.class);
        when(vertx.fileSystem()).thenReturn(files);
        when(files.mkdirs(anyString())).thenReturn(Future.succeededFuture());
        when(authentication.isSessionTokenValid("fixture-session")).thenReturn(true);
        controller = new UploadController(vertx, authentication);
    }

    @Test
    void successfulVoidWriteStillReturnsDownloadUrlAndLength() {
        when(files.writeFile(anyString(), any(Buffer.class))).thenReturn(Future.succeededFuture());
        var response = controller.upload("image", "photo.png", "Bearer fixture-session", body(new byte[]{1, 2, 3})).block();
        assertEquals(200, response.getStatusCode().value());
        assertEquals(3, response.getBody().get("size"));
        assertTrue(assertInstanceOf(String.class, response.getBody().get("url")).matches("/api/files/[a-f0-9-]{36}\\.png"));
        verify(files).writeFile(anyString(), eq(Buffer.buffer(new byte[]{1, 2, 3})));
    }

    @Test
    void unauthorizedOrExecutableImageNeverWrites() {
        assertEquals(401, controller.upload("image", "photo.png", null, body(new byte[]{1})).block().getStatusCode().value());
        assertEquals(400, controller.upload("image", "payload.html", "Bearer fixture-session", body(new byte[]{1})).block().getStatusCode().value());
        assertEquals(400, controller.upload("image", null, "Bearer fixture-session", body(new byte[]{1})).block().getStatusCode().value());
        verify(files, never()).writeFile(anyString(), any());
    }

    @Test
    void emptyAndOversizedBodiesNeverWrite() {
        assertEquals(400, controller.upload("file", "empty", "Bearer fixture-session", Flux.empty()).block().getStatusCode().value());
        assertEquals(400, controller.upload("file", "empty", "Bearer fixture-session", body(new byte[0])).block().getStatusCode().value());
        assertEquals(413, controller.upload("file", "large", "Bearer fixture-session", body(new byte[10 * 1024 * 1024 + 1]))
                .block().getStatusCode().value());
        verify(files, never()).writeFile(anyString(), any());
    }

    @Test
    void writeFailureIsNotAReportedSuccess() {
        when(files.writeFile(anyString(), any())).thenReturn(Future.failedFuture("test-write-failure"));
        assertEquals(500, controller.upload("file", "file.html", "Bearer fixture-session", body(new byte[]{1}))
                .block().getStatusCode().value());
    }

    @Test
    void unknownFilesAndTraversalFailAndBinaryIsDownloadOnly() throws Exception {
        assertEquals(400, controller.file("../file").block().getStatusCode().value());
        String name = "12345678-1234-1234-1234-123456789012.bin";
        when(files.props(anyString())).thenReturn(Future.failedFuture("not-found"));
        assertEquals(404, controller.file(name).block().getStatusCode().value());
        FileProps properties = mock(FileProps.class);
        when(properties.size()).thenReturn(4L);
        when(files.props(anyString())).thenReturn(Future.succeededFuture(properties));
        var response = controller.file(name).block();
        assertEquals(200, response.getStatusCode().value());
        assertEquals("attachment", response.getHeaders().getFirst("Content-Disposition"));
        assertEquals("nosniff", response.getHeaders().getFirst("X-Content-Type-Options"));
        assertEquals("sandbox; default-src 'none'", response.getHeaders().getFirst("Content-Security-Policy"));
        assertEquals(4, response.getHeaders().getContentLength());
    }

    private static Flux<DataBuffer> body(byte[] data) {
        return Flux.just(DefaultDataBufferFactory.sharedInstance.wrap(data));
    }
}
