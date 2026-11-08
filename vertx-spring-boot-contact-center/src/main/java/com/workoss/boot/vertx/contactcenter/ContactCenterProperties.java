package com.workoss.boot.vertx.contactcenter;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 客服中心（UCTP over WebSocket）配置。
 */
@ConfigurationProperties(prefix = "vertx.contact-center")
public class ContactCenterProperties {

    /** 是否启用 UCTP WebSocket 端点。 */
    private boolean enabled = true;

    /** WebSocket 端点路径。 */
    private String path = "/uctp";

    /** 开发级令牌表：token → 身份定义（identityId/displayName/kind/role）。生产应由身份服务替换。 */
    private Map<String, TokenSpec> tokens = new LinkedHashMap<>();

    /** 认证会话令牌有效期（分钟）。 */
    private long sessionTokenTtlMinutes = 720;

    /** Session 最后一个 Connection 结束后的重连宽限（秒，UCTP §7.3，默认 30s）。 */
    private long reconnectGraceSeconds = 30;

    /** 每 Conversation 事件日志容量（message.history 与断线补发共用）。 */
    private int journalCapacity = 256;

    /** 外部渠道绑定存储：memory（默认，进程内）/ redis（集群共享，需 spring-data-redis）。 */
    private String externalBindings = "memory";

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public Map<String, TokenSpec> getTokens() {
        return tokens;
    }

    public void setTokens(Map<String, TokenSpec> tokens) {
        this.tokens = tokens;
    }

    public long getSessionTokenTtlMinutes() {
        return sessionTokenTtlMinutes;
    }

    public void setSessionTokenTtlMinutes(long sessionTokenTtlMinutes) {
        this.sessionTokenTtlMinutes = sessionTokenTtlMinutes;
    }

    public long getReconnectGraceSeconds() {
        return reconnectGraceSeconds;
    }

    public void setReconnectGraceSeconds(long reconnectGraceSeconds) {
        this.reconnectGraceSeconds = reconnectGraceSeconds;
    }

    public int getJournalCapacity() {
        return journalCapacity;
    }

    public void setJournalCapacity(int journalCapacity) {
        this.journalCapacity = journalCapacity;
    }

    public String getExternalBindings() {
        return externalBindings;
    }

    public void setExternalBindings(String externalBindings) {
        this.externalBindings = externalBindings;
    }

    private Sql sql = new Sql();
    private List<Map<String, Object>> iceServers = new ArrayList<>();

    public Sql getSql() {
        return sql;
    }

    public void setSql(Sql sql) {
        this.sql = sql;
    }

    private Snowflake snowflake = new Snowflake();

    public Snowflake getSnowflake() {
        return snowflake;
    }

    public void setSnowflake(Snowflake snowflake) {
        this.snowflake = snowflake;
    }

    public List<Map<String, Object>> getIceServers() {
        return iceServers;
    }

    public void setIceServers(List<Map<String, Object>> iceServers) {
        this.iceServers = iceServers;
    }

    public static class Snowflake {
        /** Cluster members that must be visible before a node number is leased, guarding split-brain starts. */
        private int minMembers = 1;

        public int getMinMembers() {
            return minMembers;
        }

        public void setMinMembers(int minMembers) {
            this.minMembers = minMembers;
        }
    }

    public static class Sql {
        private String dialect = "postgresql";
        private String host = "127.0.0.1";
        private int port = 5432;
        private String database = "uctp_demo";
        private String user = "";
        private String password = "";
        private int maxSize = 8;
        private int maxWaitQueueSize = 256;

        public String getDialect() { return dialect; }
        public void setDialect(String dialect) { this.dialect = dialect; }
        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public String getDatabase() { return database; }
        public void setDatabase(String database) { this.database = database; }
        public String getUser() { return user; }
        public void setUser(String user) { this.user = user; }
        public String getPassword() { return password; }
        public void setPassword(String password) { this.password = password; }
        public int getMaxSize() { return maxSize; }
        public void setMaxSize(int maxSize) { this.maxSize = maxSize; }
        public int getMaxWaitQueueSize() { return maxWaitQueueSize; }
        public void setMaxWaitQueueSize(int maxWaitQueueSize) { this.maxWaitQueueSize = maxWaitQueueSize; }
    }

    /** 开发级令牌对应的身份定义。 */
    public static class TokenSpec {
        private String identityId;
        private String displayName;
        /** human / ai / system / external */
        private String kind = "human";
        /** 默认角色：customer / agent / supervisor / observer */
        private String role = "customer";

        public String getIdentityId() {
            return identityId;
        }

        public void setIdentityId(String identityId) {
            this.identityId = identityId;
        }

        public String getDisplayName() {
            return displayName;
        }

        public void setDisplayName(String displayName) {
            this.displayName = displayName;
        }

        public String getKind() {
            return kind;
        }

        public void setKind(String kind) {
            this.kind = kind;
        }

        public String getRole() {
            return role;
        }

        public void setRole(String role) {
            this.role = role;
        }
    }
}
