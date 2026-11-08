package com.workoss.boot.vertx.contactcenter;

import com.workoss.boot.vertx.contactcenter.channel.ChannelConnector;
import com.workoss.boot.vertx.contactcenter.channel.ChannelRegistry;
import com.workoss.boot.vertx.contactcenter.channel.ExternalBindingStore;
import com.workoss.boot.vertx.contactcenter.channel.InMemoryExternalBindingStore;
import com.workoss.boot.vertx.contactcenter.channel.RedisExternalBindingStore;
import com.workoss.boot.vertx.contactcenter.core.ConversationService;
import com.workoss.boot.vertx.contactcenter.core.UctpEventRouter;
import com.workoss.boot.vertx.contactcenter.journal.SqlJournalRepository;
import com.workoss.boot.vertx.contactcenter.journal.SqlConversationRepository;
import com.workoss.boot.vertx.contactcenter.journal.JournalRepository;
import com.workoss.boot.vertx.contactcenter.routing.WorkforceService;
import com.workoss.boot.vertx.contactcenter.uctp.ClusterSnowflakeService;
import com.workoss.boot.vertx.contactcenter.uctp.SnowflakeGenerator;
import com.workoss.boot.vertx.contactcenter.uctp.UctpEnvelope;
import com.workoss.boot.vertx.contactcenter.uctp.UctpIds;
import com.workoss.boot.vertx.web.server.websocket.ClusterWebSocketBroadcaster;
import com.workoss.boot.vertx.web.server.websocket.VertxWebSocketAutoConfiguration;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.mysqlclient.MySQLBuilder;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.pgclient.PgBuilder;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.server.RouterFunction;
import org.springframework.web.reactive.function.server.RouterFunctions;
import org.springframework.web.reactive.function.server.ServerResponse;
import org.springframework.web.reactive.handler.SimpleUrlHandlerMapping;
import org.springframework.web.reactive.socket.WebSocketHandler;

import java.util.concurrent.TimeUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 客服中心（UCTP over WebSocket）自动配置。
 *
 * <p>依赖 web-server 的 {@link ClusterWebSocketBroadcaster}（WebSocket 升级 + 集群扇出）；
 * 端点路径由 {@code vertx.contact-center.path} 决定（默认 {@code /uctp}）。
 */
@AutoConfiguration(after = VertxWebSocketAutoConfiguration.class)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.REACTIVE)
@ConditionalOnBean(ClusterWebSocketBroadcaster.class)
@ConditionalOnProperty(prefix = "vertx.contact-center", name = "enabled", havingValue = "true", matchIfMissing = true)
@EnableConfigurationProperties(ContactCenterProperties.class)
public class ContactCenterAutoConfiguration {

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean(Pool.class)
    public Pool uctpSqlPool(Vertx vertx, ContactCenterProperties properties) {
        ContactCenterProperties.Sql sql = properties.getSql();
        if (sql.getMaxSize() < 1 || sql.getMaxWaitQueueSize() < 1) {
            throw new IllegalArgumentException("SQL pool limits must be positive");
        }
        PoolOptions pool = new PoolOptions().setMaxSize(sql.getMaxSize())
                .setMaxWaitQueueSize(sql.getMaxWaitQueueSize());
        if ("postgresql".equalsIgnoreCase(sql.getDialect())) {
            PgConnectOptions options = new PgConnectOptions().setHost(sql.getHost()).setPort(sql.getPort())
                    .setDatabase(sql.getDatabase()).setUser(sql.getUser()).setPassword(sql.getPassword())
                    .setCachePreparedStatements(true).setPreparedStatementCacheMaxSize(256);
            return PgBuilder.pool().using(vertx).connectingTo(options).with(pool).build();
        }
        if ("mysql".equalsIgnoreCase(sql.getDialect()) || "polardbx".equalsIgnoreCase(sql.getDialect())) {
            MySQLConnectOptions options = new MySQLConnectOptions().setHost(sql.getHost()).setPort(sql.getPort())
                    .setDatabase(sql.getDatabase()).setUser(sql.getUser()).setPassword(sql.getPassword())
                    .setCachePreparedStatements(true).setPreparedStatementCacheMaxSize(256);
            return MySQLBuilder.pool().using(vertx).connectingTo(options).with(pool).build();
        }
        throw new IllegalArgumentException("Unsupported SQL dialect: " + sql.getDialect());
    }

    @Bean(destroyMethod = "close")
    @ConditionalOnMissingBean
    public ClusterSnowflakeService clusterSnowflakeService(Vertx vertx, ContactCenterProperties properties) {
        return new ClusterSnowflakeService(vertx, properties.getSnowflake().getMinMembers());
    }

    @Bean
    @ConditionalOnMissingBean
    public SnowflakeGenerator snowflakeGenerator(ClusterSnowflakeService service) {
        if (Context.isOnEventLoopThread()) {
            throw new IllegalStateException("Snowflake initialization must run outside the event loop");
        }
        return service.start().toCompletionStage().toCompletableFuture().join();
    }

    @Bean
    @ConditionalOnMissingBean
    public UctpIds uctpIds(SnowflakeGenerator generator) {
        return new UctpIds(generator);
    }

    @Bean
    @ConditionalOnMissingBean
    public UctpEnvelope uctpEnvelope(UctpIds ids) {
        return new UctpEnvelope(ids);
    }

    @Bean
    @ConditionalOnMissingBean(name = "uctpIdsRouter")
    public RouterFunction<ServerResponse> uctpIdsRouter(ContactCenterProperties properties, UctpIds ids) {
        return RouterFunctions.route().GET(properties.getPath() + "/ids", request -> {
            int count;
            try {
                count = Integer.parseInt(request.queryParam("count").orElse("128"));
            } catch (NumberFormatException e) {
                return ServerResponse.badRequest().cacheControl(CacheControl.noStore()).build();
            }
            if (count < 1 || count > 256) {
                return ServerResponse.badRequest().cacheControl(CacheControl.noStore()).build();
            }
            List<String> allocated = new ArrayList<>(count);
            try {
                for (int i = 0; i < count; i++) {
                    allocated.add(Long.toString(ids.nextId()));
                }
            } catch (IllegalStateException e) {
                return ServerResponse.status(HttpStatus.SERVICE_UNAVAILABLE)
                        .cacheControl(CacheControl.noStore()).build();
            }
            return ServerResponse.ok().contentType(MediaType.APPLICATION_JSON)
                    .cacheControl(CacheControl.noStore()).bodyValue(Map.of("ids", allocated));
        }).build();
    }

    @Bean
    @ConditionalOnMissingBean
    public UctpEventRouter uctpEventRouter(Vertx vertx, ClusterWebSocketBroadcaster broadcaster) {
        UctpEventRouter router = new UctpEventRouter(vertx, broadcaster);
        router.registerListener();
        return router;
    }

    @Bean
    @ConditionalOnMissingBean
    public ConversationService conversationService(Vertx vertx, UctpEventRouter router,
                                                   ContactCenterProperties properties,
                                                   UctpIds ids, UctpEnvelope envelopes,
                                                   ObjectProvider<ChannelRegistry> channelRegistry,
                                                   ObjectProvider<JournalRepository> journalRepository,
                                                   SqlConversationRepository conversationRepository,
                                                   ObjectProvider<ExternalBindingStore> bindingStore) {
        ConversationService service = new ConversationService(vertx, router, properties, ids, envelopes);
        channelRegistry.ifAvailable(service::channelRegistry);
        journalRepository.ifAvailable(service::journalRepository);
        service.conversationRepository(conversationRepository);
        bindingStore.ifAvailable(service::bindingStore);
        service.registerNodeConsumers();
        return service;
    }

    @Bean
    @ConditionalOnMissingBean
    public WorkforceService workforceService(Vertx vertx, UctpEventRouter router,
                                             ConversationService conversationService,
                                             UctpIds ids, UctpEnvelope envelopes) {
        WorkforceService service = new WorkforceService(vertx, router, conversationService, ids, envelopes);
        conversationService.workforceService(service);
        service.registerListener();
        return service;
    }

    /** 渠道连接器注册表：容器内所有 ChannelConnector Bean 自动收录。 */
    @Bean
    @ConditionalOnMissingBean
    public ChannelRegistry channelRegistry(ObjectProvider<ChannelConnector> connectors) {
        return new ChannelRegistry(connectors.stream().toList());
    }

    /**
     * 外部渠道绑定存储：{@code vertx.contact-center.external-bindings=redis} 且容器有
     * StringRedisTemplate 时用 Redis（集群共享），否则进程内。
     */
    @Bean
    @ConditionalOnMissingBean
    public ExternalBindingStore externalBindingStore(ObjectProvider<StringRedisTemplate> redis,
                                                     ContactCenterProperties properties) {
        StringRedisTemplate template = redis.getIfAvailable();
        if ("redis".equalsIgnoreCase(properties.getExternalBindings()) && template != null) {
            return new RedisExternalBindingStore(template);
        }
        return new InMemoryExternalBindingStore();
    }

    @Bean
    @ConditionalOnMissingBean(JournalRepository.class)
    public SqlJournalRepository uctpJournalRepository(Pool pool, ContactCenterProperties properties) {
        SqlJournalRepository repository = new SqlJournalRepository(pool,
                "postgresql".equalsIgnoreCase(properties.getSql().getDialect()));
        repository.initialize().toCompletionStage().toCompletableFuture().orTimeout(15, TimeUnit.SECONDS).join();
        return repository;
    }

    @Bean
    @ConditionalOnMissingBean
    public SqlConversationRepository uctpConversationRepository(Pool pool, ContactCenterProperties properties) {
        SqlConversationRepository repository = new SqlConversationRepository(pool,
                "postgresql".equalsIgnoreCase(properties.getSql().getDialect()));
        repository.initialize().toCompletionStage().toCompletableFuture().orTimeout(15, TimeUnit.SECONDS).join();
        return repository;
    }

    @Bean
    public RouterFunction<ServerResponse> uctpConfigRouter(ContactCenterProperties properties,
                                                           ContactCenterWebSocketHandler handler) {
        return RouterFunctions.route().GET(properties.getPath() + "/config", request -> {
            boolean credentials = properties.getIceServers().stream().anyMatch(server -> server.containsKey("credential"));
            String authorization = request.headers().firstHeader("Authorization");
            String token = authorization != null && authorization.startsWith("Bearer ") ? authorization.substring(7) : null;
            if (credentials && !handler.isSessionTokenValid(token)) {
                return ServerResponse.status(HttpStatus.UNAUTHORIZED).cacheControl(CacheControl.noStore()).build();
            }
            return ServerResponse.ok().cacheControl(CacheControl.noStore()).bodyValue(Map.of(
                    "ice_servers", properties.getIceServers(), "max_upload_bytes", 10 * 1024 * 1024));
        }).build();
    }

    @Bean
    public ContactCenterWebSocketHandler contactCenterWebSocketHandler(
            Vertx vertx, UctpEventRouter router, ConversationService conversationService,
            WorkforceService workforceService, ContactCenterProperties properties, UctpEnvelope envelopes) {
        return new ContactCenterWebSocketHandler(vertx, router, conversationService, workforceService,
                properties, envelopes);
    }

    /** 把 UCTP 端点挂到 {@code vertx.contact-center.path}；顺序高于静态资源映射。 */
    @Bean
    public SimpleUrlHandlerMapping contactCenterWebSocketMapping(
            ContactCenterWebSocketHandler handler, ContactCenterProperties properties) {
        Map<String, Object> urlMap = new LinkedHashMap<>();
        urlMap.put(properties.getPath(), handler);
        SimpleUrlHandlerMapping mapping = new SimpleUrlHandlerMapping();
        mapping.setUrlMap(urlMap);
        mapping.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return mapping;
    }
}
