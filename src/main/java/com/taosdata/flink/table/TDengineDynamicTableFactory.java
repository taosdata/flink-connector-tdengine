package com.taosdata.flink.table;

import com.google.common.base.Strings;
import com.taosdata.flink.common.TDengineConfigParams;
import com.taosdata.jdbc.TSDBDriver;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

import static com.taosdata.flink.table.TDengineConnectorOptions.SINK_PARALLELISM;

public class TDengineDynamicTableFactory implements DynamicTableSourceFactory, DynamicTableSinkFactory {

    public static final String IDENTIFIER = "tdengine-connector";

    private static final Logger LOG = LoggerFactory.getLogger(TDengineDynamicTableFactory.class);

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        helper.validate();
        validateSink(config);
        Integer parallelism = helper.getOptions().get(SINK_PARALLELISM);
        TableSchema physicalSchema = TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, config.get(TDengineConnectorOptions.CHARSET));
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_LOCALE, config.get(TDengineConnectorOptions.LOCALE));
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_TIME_ZONE, config.get(TDengineConnectorOptions.SERVER_TIME_ZONE));
        connProps.setProperty(TDengineConfigParams.TD_DATABASE_NAME, config.get(TDengineConnectorOptions.SINK_DBNAME_NAME));
        connProps.setProperty(TDengineConfigParams.TD_SUPERTABLE_NAME, config.get(TDengineConnectorOptions.SINK_SUPERTABLE_NAME));
        connProps.setProperty(TDengineConfigParams.TD_TABLE_NAME, config.get(TDengineConnectorOptions.SINK_TABLE_NAME));
        connProps.setProperty(TDengineConfigParams.TD_JDBC_URL, config.get(TDengineConnectorOptions.TD_JDBC_URL));
        connProps.setProperty(TDengineConfigParams.TD_BATCH_SIZE, "" + config.get(TDengineConnectorOptions.SINK_BATCH_SIZE));
        connProps.setProperty(TDengineConfigParams.VALUE_DESERIALIZER, "RowData");
        try {
            return new TDengineTableSink(connProps, physicalSchema, parallelism);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        final ReadableConfig config = helper.getOptions();
        helper.validate();
        String mode = config.get(TDengineConnectorOptions.TD_JDBC_MODE);
        if (!Strings.isNullOrEmpty(mode)) {
            if (mode.compareTo("source") == 0) {
                return createTableSource(config, context);
            } else if (mode.compareTo("cdc") == 0){
                return createTableCdc(config);
            }
        }

        throw new ValidationException(
                String.format(
                        "The tdengine jdbc mode are invalid！"));
    }

    private static void validateSink(ReadableConfig tableOptions) {
        Optional<String> jdbcUrl = tableOptions.getOptional(TDengineConnectorOptions.TD_JDBC_URL);
        Optional<String> dbname = tableOptions.getOptional(TDengineConnectorOptions.SINK_DBNAME_NAME);
        Optional<String> tableName = tableOptions.getOptional(TDengineConnectorOptions.SINK_TABLE_NAME);
        Optional<String> superName = tableOptions.getOptional(TDengineConnectorOptions.SINK_SUPERTABLE_NAME);

        if (!jdbcUrl.isPresent() || Strings.isNullOrEmpty(jdbcUrl.get())) {
            throw new ValidationException("Option 'jdbcUrl' must be set.");
        }

        if (!dbname.isPresent() || Strings.isNullOrEmpty(dbname.get())) {
            throw new ValidationException("dbname must be set.");
        }

        if ((!tableName.isPresent() || Strings.isNullOrEmpty(tableName.get())) &&
                (!superName.isPresent() || Strings.isNullOrEmpty(superName.get()))) {
            throw new ValidationException("tableName or superTableName must be set.");
        }

    }


    private static void validateSource(ReadableConfig tableOptions) {
        Optional<String> jdbcUrl = tableOptions.getOptional(TDengineConnectorOptions.TD_JDBC_URL);
        Optional<String> scanQuery = tableOptions.getOptional(TDengineConnectorOptions.SCAN_QUERY);

        if (!jdbcUrl.isPresent() || Strings.isNullOrEmpty(jdbcUrl.get())) {
            throw new ValidationException("Option 'jdbcUrl' must be set.");
        }

        if (!scanQuery.isPresent() || Strings.isNullOrEmpty(scanQuery.get())) {
            throw new ValidationException("scan_query must be set.");
        }
    }

    private DynamicTableSource createTableSource(ReadableConfig config, Context context) {
        validateSource(config);
        String url = config.get(TDengineConnectorOptions.TD_JDBC_URL);
        String scanQurey = config.get(TDengineConnectorOptions.SCAN_QUERY);
        final DataType physicalDataType = context.getPhysicalRowDataType();

        Properties connProps = new Properties();
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_ENABLE_AUTO_RECONNECT, config.get(TDengineConnectorOptions.ENABLE_AUTO_RECONNECT));
        connProps.setProperty(TSDBDriver.PROPERTY_KEY_CHARSET, config.get(TDengineConnectorOptions.CHARSET));
        return new TDengineTableSource(url, scanQurey, physicalDataType, connProps);
    }

    private static void validateCdc(ReadableConfig tableOptions) {
        Optional<String> topic = tableOptions.getOptional(TDengineConnectorOptions.TOPIC);
        Optional<String> groupId = tableOptions.getOptional(TDengineConnectorOptions.GROUP_ID);
        Optional<String> server = tableOptions.getOptional(TDengineConnectorOptions.BOOTSTRAP_SERVERS);
        if (!topic.isPresent() || Strings.isNullOrEmpty(topic.get())) {
            throw new ValidationException("Option 'topic' must be set.");
        }

        if (!groupId.isPresent() || Strings.isNullOrEmpty(groupId.get())) {
            throw new ValidationException("Option 'group.id' must be set.");
        }
        if (!server.isPresent() || Strings.isNullOrEmpty(server.get())) {
            throw new ValidationException("Option 'bootstrap.servers' must be set.");
        }
    }

    private DynamicTableSource createTableCdc(ReadableConfig config) {
        validateCdc(config);
        Properties properties = new Properties();
        properties.setProperty("td.connect.type", "ws");
        String optionVal = config.get(TDengineConnectorOptions.BOOTSTRAP_SERVERS);
        properties.setProperty("bootstrap.servers", optionVal);
        optionVal = config.get(TDengineConnectorOptions.AUTO_OFFSET_RESET);
        properties.setProperty("auto.offset.reset", optionVal);
        optionVal = config.get(TDengineConnectorOptions.GROUP_ID);
        properties.setProperty("group.id", optionVal);
        optionVal = config.get(TDengineConnectorOptions.USERNAME);
        properties.setProperty("td.connect.user", optionVal);
        optionVal = config.get(TDengineConnectorOptions.PASSWORD);
        properties.setProperty("td.connect.pass", optionVal);
        optionVal = config.get(TDengineConnectorOptions.ENABLE_AUTO_COMMIT);
        properties.setProperty("enable.auto.commit", optionVal);
        properties.setProperty("value.deserializer", "RowData");
        properties.setProperty("value.deserializer.encoding", config.get(TDengineConnectorOptions.CHARSET));
        String topic = config.get(TDengineConnectorOptions.TOPIC);
        return new TDengineTableCdc(topic, properties);
    }
    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(TDengineConnectorOptions.TD_JDBC_MODE);
        return requiredOptions;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();
        optionalOptions.add(TDengineConnectorOptions.TD_JDBC_URL);
        optionalOptions.add(TDengineConnectorOptions.HOSTNAME);
        optionalOptions.add(TDengineConnectorOptions.PORT);
        optionalOptions.add(TDengineConnectorOptions.USERNAME);
        optionalOptions.add(TDengineConnectorOptions.PASSWORD);
        optionalOptions.add(TDengineConnectorOptions.DATABASE_NAME);
        optionalOptions.add(TDengineConnectorOptions.TABLE_NAME);
        optionalOptions.add(TDengineConnectorOptions.SCAN_QUERY);
        optionalOptions.add(TDengineConnectorOptions.BOOTSTRAP_SERVERS);
        optionalOptions.add(TDengineConnectorOptions.TOPIC);
        optionalOptions.add(TDengineConnectorOptions.GROUP_ID);
        optionalOptions.add(TDengineConnectorOptions.CLIENT_ID);
        optionalOptions.add(TDengineConnectorOptions.AUTO_OFFSET_RESET);
        optionalOptions.add(TDengineConnectorOptions.ENABLE_AUTO_COMMIT);
        optionalOptions.add(TDengineConnectorOptions.AUTO_COMMIT_INTERVAL);
        optionalOptions.add(TDengineConnectorOptions.SINK_DBNAME_NAME);
        optionalOptions.add(TDengineConnectorOptions.SINK_TABLE_NAME);
        optionalOptions.add(TDengineConnectorOptions.SINK_SUPERTABLE_NAME);
        optionalOptions.add(TDengineConnectorOptions.SINK_BATCH_SIZE);
        optionalOptions.add(TDengineConnectorOptions.LOCALE);
        optionalOptions.add(TDengineConnectorOptions.CHARSET);
        optionalOptions.add(TDengineConnectorOptions.ENABLE_AUTO_RECONNECT);
        optionalOptions.add(TDengineConnectorOptions.SERVER_TIME_ZONE);
        return optionalOptions;
    }


}