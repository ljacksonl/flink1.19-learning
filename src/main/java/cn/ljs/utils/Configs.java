package cn.ljs.utils;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Github> https://github.com/typesafehub/config
 * <p>
 * ## 关于 withFallback 方法说明
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * 1. a.withFallback(b) //a和b合并，如果有相同的key，以a为准
 * 2. a.withOnlyPath(String path) //只取a里的path下的配置
 * 3. a.withoutPath(String path) //只取a里出path外的配置
 * <p>
 * 参考 link Builder#with... 相关方法正确合并配置文件，目前未提供
 * <p>
 * ## 推荐配置参数优先级
 * ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
 * 代码级别 Config > 配置文件*.conf > system_properties > system_environment > default.conf
 * <p>
 * ## 仅识别固定前缀字符变量，使用{@link ConfigHelper#usePrefix}
 * ## 识别环境配置，使用{@link ConfigHelper#systemEnvValName}
 *
 * @author wei.Li by 2019-04-10
 */
public class Configs {

    /**
     * This config has all of the JVM system properties including any custom -D properties
     */
    private static final Config SYSTEM_PROPERTIES = ConfigFactory.systemProperties();

    /**
     * This config has access to all of the environment variables
     */
    private static final Config SYSTEM_ENVIRONMENT = ConfigFactory.systemEnvironment();

    /**
     * Always start with a blank config and add fallbacks
     */
    private static final AtomicReference<Config> PROPERTIES_REF = new AtomicReference<>(null);

    private Configs() {
    }

    /**
     * Init properties.
     *
     * @param config the config
     */
    public static void initProperties(Config config) {
        boolean success = PROPERTIES_REF.compareAndSet(null, config);
        if (!success) {
            throw new RuntimeException("propertiesRef Config has already been initialized. This should only be called once.");
        }
    }

    /**
     * Properties config.
     *
     * @return the config
     */
    public static Config properties() {
        return PROPERTIES_REF.get();
    }

    /**
     * System properties config.
     *
     * @return the config
     */
    public static Config systemProperties() {
        return SYSTEM_PROPERTIES;
    }

    /**
     * System environment config.
     *
     * @return the config
     */
    public static Config systemEnvironment() {
        return SYSTEM_ENVIRONMENT;
    }

    /**
     * This should return the current executing user path
     *
     * @return String execution directory
     */
    public static String getExecutionDirectory() {
        return SYSTEM_PROPERTIES.getString("user.dir");
    }

    public static void printlnConfig(Config config) {
        config.entrySet().forEach(e -> System.out.println(e.getKey() + "=" + e.getValue().render()));
    }

    /**
     * The type ConfigHelper.
     */
    public static class ConfigHelper {

        private static final Logger LOG = LoggerFactory.getLogger(ConfigHelper.class);
        private static final String SYSTEM_ENV_VAL_NAME_FORMAT = "%env";

        /**
         * 固定前缀字段
         * eg. usePrefix=conf
         * 则最终解析后仅返回以 conf 开头变量
         */
        @Nullable
        private String usePrefix;
        /**
         * 使用环境变量名称替换路径参数
         * 提取环境变量名称优先级 System.getProperties > System.getenv
         * eg.
         * path=/conf/%env/app.conf , systemEnvValName=env env=dev , 结果为 path=/conf/dev/app.conf
         * path=/conf/app-%env.conf , systemEnvValName=env env=dev , 结果为 path=/conf/app-dev.conf
         *
         * @see #buildEnvPath(String)
         */
        @Nullable
        private String systemEnvValName;
        private Config conf = ConfigFactory.empty();

        public ConfigHelper() {
        }
        public ConfigHelper(String usePrefix) {
            this.usePrefix = usePrefix;
        }
        public ConfigHelper(String usePrefix, String systemEnvValName) {
            this.usePrefix = usePrefix;
            this.systemEnvValName = systemEnvValName;
            LOG.info("Loading configs first row is highest priority, second row is fallback and so on");
        }

        /**
         * 路径内容替换
         * 提换逻辑参考 {@link #systemEnvValName} 详细描述
         *
         * @param path path
         * @return path
         */
        private String buildEnvPath(String path) {
            if (systemEnvValName == null) {
                return path;
            }
            final String p = System.getProperty(systemEnvValName);
            final String e = System.getenv(systemEnvValName);
            if (p == null && e == null) {
                throw new NullPointerException("systemEnvValName " + systemEnvValName + " null");
            }
            if (!path.contains(SYSTEM_ENV_VAL_NAME_FORMAT)) {
                throw new IllegalArgumentException("path not found " + SYSTEM_ENV_VAL_NAME_FORMAT + " , path:" + path);
            }
            return p == null ?
                    path.replaceAll(SYSTEM_ENV_VAL_NAME_FORMAT, e) :
                    path.replaceAll(SYSTEM_ENV_VAL_NAME_FORMAT, p);
        }

        /**
         * With fallback resource builder.
         *
         * @param resource the resource
         * @return the builder
         */
        public ConfigHelper withFallbackResource(String resource) {
            final Config resourceConfig = ConfigFactory.parseResources(this.buildEnvPath(resource));
            final String empty = resourceConfig.entrySet().isEmpty() ? " contains no values" : "";
            conf = conf.withFallback(resourceConfig);
            LOG.info("Loaded config file from resource ({}){}", resource, empty);
            return this;
        }

        /**
         * With fallback system properties builder.
         *
         * @return the builder
         */
        public ConfigHelper withFallbackSystemProperties() {
            conf = conf.withFallback(systemProperties());
            LOG.info("Loaded system properties into config");
            return this;
        }

        /**
         * With fallback system environment builder.
         *
         * @return the builder
         */
        public ConfigHelper withFallbackSystemEnvironment() {
            conf = conf.withFallback(systemEnvironment());
            LOG.info("Loaded system environment into config");
            return this;
        }

        /**
         * With fallback optional file builder.
         *
         * @param path the path
         * @return the builder
         */
        public ConfigHelper withFallbackOptionalFile(String path) {
            final File secureConfFile = new File(this.buildEnvPath(path));
            if (secureConfFile.exists()) {
                LOG.info("Loaded config file from path ({})", path);
                conf = conf.withFallback(ConfigFactory.parseFile(secureConfFile));
            } else {
                LOG.info("Attempted to load file from path ({}) but it was not found", path);
            }
            return this;
        }

        /**
         * With fallback optional relative file builder.
         *
         * @param path the path
         * @return the builder
         */
        public ConfigHelper withFallbackOptionalRelativeFile(String path) {
            return this.withFallbackOptionalFile(getExecutionDirectory() + path);
        }

        /**
         * With fallback config builder.
         *
         * @param config the config
         * @return the builder
         */
        public ConfigHelper withFallbackConfig(Config config) {
            conf = conf.withFallback(config);
            return this;
        }

        /**
         * Build config.
         * resolve > filter usePrefix
         *
         * @return the config
         */
        public Config build() {
            // Resolve substitutions.
            conf = conf.resolve();

            //过滤固定前缀变量
            final Set<Map.Entry<String, ConfigValue>> entries = conf.entrySet();
            final Map<String, ConfigValue> cp = new HashMap<>(entries.size());
            entries.forEach(e -> {
                final String key = e.getKey();
                if (key.startsWith(usePrefix)) {
                    cp.put(key, e.getValue());
                }
            });

            final Config config = ConfigFactory.parseMap(cp);
            if (LOG.isDebugEnabled()) {
                LOG.debug("Logging properties. Make sure sensitive data such as passwords or secrets are not logged!");
                LOG.debug(config.root().render());
            }
            return config;
        }
    }
}

