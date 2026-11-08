package com.workoss.boot.vertx.contact;

import com.workoss.boot.vertx.contactcenter.ContactCenterWebSocketHandler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferLimitException;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.net.MalformedURLException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@RestController
public class UploadController {

    private static final int MAX_SIZE = 10 * 1024 * 1024;
    private static final Map<String, Set<String>> EXTENSIONS = Map.of(
            "image", Set.of(".png", ".jpg", ".jpeg", ".webp", ".gif", ".avif"),
            "video", Set.of(".mp4", ".webm"), "audio", Set.of(".m4a", ".mp3", ".wav", ".ogg", ".webm"));
    private static final Map<String, String> MIME_TYPES = Map.ofEntries(
            Map.entry(".png", "image/png"), Map.entry(".jpg", "image/jpeg"), Map.entry(".jpeg", "image/jpeg"),
            Map.entry(".webp", "image/webp"), Map.entry(".gif", "image/gif"), Map.entry(".avif", "image/avif"),
            Map.entry(".mp4", "video/mp4"), Map.entry(".webm", "video/webm"), Map.entry(".m4a", "audio/mp4"),
            Map.entry(".mp3", "audio/mpeg"), Map.entry(".wav", "audio/wav"), Map.entry(".ogg", "audio/ogg"));

    private final Path dir = Paths.get("uploaded").toAbsolutePath().normalize();
    private final Vertx vertx;
    private final ContactCenterWebSocketHandler authentication;

    public UploadController(Vertx vertx, ContactCenterWebSocketHandler authentication) {
        this.vertx = vertx;
        this.authentication = authentication;
        vertx.fileSystem().mkdirs(dir.toString()).toCompletionStage().toCompletableFuture()
                .orTimeout(10, TimeUnit.SECONDS).join();
    }

    @PostMapping("/api/upload")
    public Mono<ResponseEntity<Map<String, Object>>> upload(@RequestParam("kind") String kind,
            @RequestParam(value = "filename", required = false) String filename,
            @RequestHeader(value = HttpHeaders.AUTHORIZATION, required = false) String authorization,
            @RequestBody(required = false) Flux<DataBuffer> body) {
        String token = authorization != null && authorization.startsWith("Bearer ") ? authorization.substring(7) : null;
        if (!authentication.isSessionTokenValid(token)) {
            return Mono.just(ResponseEntity.status(401).body(Map.<String, Object>of("error", "unauthenticated")));
        }
        String extension = extensionOf(filename, kind);
        if (extension == null) {
            return Mono.just(ResponseEntity.badRequest().body(Map.<String, Object>of("error", "unsupported-file-type")));
        }
        return DataBufferUtils.join(body, MAX_SIZE).flatMap(data -> {
            byte[] bytes = new byte[data.readableByteCount()];
            data.read(bytes);
            DataBufferUtils.release(data);
            if (bytes.length == 0) return Mono.just(ResponseEntity.badRequest().body(Map.<String, Object>of("error", "empty-body")));
            String name = UUID.randomUUID() + extension;
            return Mono.fromCompletionStage(vertx.fileSystem().writeFile(dir.resolve(name).toString(), Buffer.buffer(bytes))
                    .toCompletionStage()).thenReturn(ResponseEntity.ok(Map.<String, Object>of("url", "/api/files/" + name,
                            "size", bytes.length)));
        }).switchIfEmpty(Mono.just(ResponseEntity.badRequest().body(Map.<String, Object>of("error", "empty-body"))))
                .onErrorResume(DataBufferLimitException.class,
                        failure -> Mono.just(ResponseEntity.status(413).body(Map.<String, Object>of("error", "file-too-large"))))
                .onErrorResume(failure -> Mono.just(ResponseEntity.internalServerError().body(Map.<String, Object>of("error", "write-failed"))));
    }

    @GetMapping("/api/files/{name}")
    public Mono<ResponseEntity<Resource>> file(@PathVariable("name") String name) throws MalformedURLException {
        if (!name.matches("[a-f0-9-]{36}\\.[a-z0-9]{1,10}")) {
            return Mono.just(ResponseEntity.badRequest().build());
        }
        Path path = dir.resolve(name);
        Resource resource = new UrlResource(path.toUri());
        String extension = name.substring(name.lastIndexOf('.'));
        String mime = MIME_TYPES.getOrDefault(extension, "application/octet-stream");
        return Mono.fromCompletionStage(vertx.fileSystem().props(path.toString()).toCompletionStage())
                .map(properties -> ResponseEntity.ok().contentType(MediaType.parseMediaType(mime))
                        .contentLength(properties.size()).header("X-Content-Type-Options", "nosniff")
                        .header("Content-Security-Policy", "sandbox; default-src 'none'")
                        .header(HttpHeaders.CACHE_CONTROL, "private, no-store")
                        .header(HttpHeaders.CONTENT_DISPOSITION, extension.equals(".bin") ? "attachment" : "inline")
                        .body(resource))
                .onErrorResume(failure -> Mono.just(ResponseEntity.notFound().build()));
    }

    private String extensionOf(String filename, String kind) {
        if ("file".equals(kind)) return ".bin";
        Set<String> allowed = EXTENSIONS.get(kind);
        if (allowed == null) return null;
        int dot = filename == null ? -1 : filename.lastIndexOf('.');
        String extension = dot < 0 ? null : filename.substring(dot).toLowerCase(java.util.Locale.ROOT);
        return extension != null && allowed.contains(extension) ? extension : null;
    }
}
