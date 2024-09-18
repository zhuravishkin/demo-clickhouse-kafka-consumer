package com.zhuravishkin.demo_clickhouse_kafka_consumer;

import com.clickhouse.jdbc.ClickHouseDataSource;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

@Slf4j
@SpringBootApplication
public class ClickhouseKafkaConsumerApplication implements CommandLineRunner {
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://192.168.1.17:18123/zhuravishkin";
    private static final String COLUMN_TYPE = "Nullable(String)";

    public static void main(String[] args) {
        SpringApplication.run(ClickhouseKafkaConsumerApplication.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        String s = Files.readString(Path.of("src/main/resources/templates/config1.json"));
        System.out.println(s);

        processTableFromJson(s);
    }

    public static void processTableFromJson(String json) throws Exception {
        try {
            String tableName = "cep";
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(json);
            System.out.println("payload: " + rootNode);

            ClickHouseDataSource dataSource = new ClickHouseDataSource(CLICKHOUSE_URL, new Properties());
            try (Connection conn = dataSource.getConnection("username", "password");
                 Statement stmt = conn.createStatement()) {
                boolean tableExists = isTableExist(stmt, tableName);
                System.out.println("tableExists: " + tableExists);

                Map<String, String> columns = extractColumns(rootNode);

                if (!tableExists) {
                    createTable(stmt, tableName, columns);
                    createErrorTable(stmt, tableName);
                    createKafkaTable(stmt, tableName);
                    createMaterializedView(stmt, tableName, columns);
                    createErrorMaterializedView(stmt, tableName);
                } else {
                    Map<String, String> newColumns = addMissingColumns(stmt, tableName, columns);
                    updateMaterializedView(stmt, tableName, columns, newColumns);
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        }
    }

    private static boolean isTableExist(Statement statement, String tableName) throws Exception {
        String query = "EXISTS TABLE " + tableName;
        ResultSet resultSet = statement.executeQuery(query);
        if (resultSet.next()) {
            return resultSet.getBoolean(1);
        }

        return false;
    }

    private static void createTable(Statement statement, String tableName, Map<String, String> columns) throws Exception {
        StringBuilder createTableQuery = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");

        for (Map.Entry<String, String> entry : columns.entrySet()) {
            createTableQuery.append(entry.getKey()).append(" ").append(entry.getValue()).append(", ");
        }

        createTableQuery.append("_timestamp DateTime DEFAULT now()) ENGINE = MergeTree() ORDER BY _timestamp ");
        createTableQuery.append("TTL _timestamp + INTERVAL 3 DAY");
        System.out.println(createTableQuery);

        statement.execute(createTableQuery.toString());
        System.out.println("table created: " + tableName);
    }

    private static void createErrorTable(Statement statement, String tableName) throws SQLException {
        String errorTableName = tableName + "_errors";
        String createErrorTableQuery = "CREATE TABLE IF NOT EXISTS " + errorTableName + " (" +
                "payload String, " +
                "error_message String, " +
                "_timestamp DateTime DEFAULT now()" +
                ") ENGINE = MergeTree() ORDER BY _timestamp";

        statement.execute(createErrorTableQuery);
    }

    private static Map<String, String> extractColumns(JsonNode node) {
        Map<String, String> columns = new HashMap<>();
        extractColumnsRecursive("", node, columns);

        return columns;
    }

    private static void extractColumnsRecursive(String prefix, JsonNode node, Map<String, String> columns) {
        if (node.isObject()) {
            Iterator<Map.Entry<String, JsonNode>> fields = node.fields();
            while (fields.hasNext()) {
                Map.Entry<String, JsonNode> field = fields.next();
                String key = field.getKey();
                JsonNode valueNode = field.getValue();

                String fullKey = prefix.isEmpty() ? key : prefix + "__" + key;

                String type = valueNode.path("type").asText();

                if ("object".equals(type)) {
                    JsonNode fieldsNode = valueNode.path("fields");
                    if (fieldsNode != null && fieldsNode.isObject()) {
                        extractColumnsRecursive(fullKey, fieldsNode, columns);
                    }
                } else {
                    columns.put(fullKey, COLUMN_TYPE);
                }
            }
        }
    }

    private static Map<String, String> addMissingColumns(Statement statement, String tableName, Map<String, String> columns) throws Exception {
        Set<String> existingColumns = getExistingColumns(statement, tableName);

        Map<String, String> newColumns = new HashMap<>();
        for (Map.Entry<String, String> entry : columns.entrySet()) {
            if (!existingColumns.contains(entry.getKey())) {
                newColumns.put(entry.getKey(), entry.getValue());
            }
        }

        if (!newColumns.isEmpty()) {
            for (Map.Entry<String, String> entry : newColumns.entrySet()) {
                String alterTableQuery = "ALTER TABLE " + tableName + " ADD COLUMN " + entry.getKey() + " " + entry.getValue();
                statement.execute(alterTableQuery);
                System.out.println("Added column: " + entry.getKey());
            }
        }

        return newColumns;
    }

    private static Set<String> getExistingColumns(Statement statement, String tableName) throws SQLException {
        String query = "DESCRIBE TABLE " + tableName;
        Set<String> columns = new HashSet<>();

        try (ResultSet resultSet = statement.executeQuery(query)) {
            while (resultSet.next()) {
                String column = resultSet.getString(1);
                columns.add(column);
            }
        }

        return columns;
    }

    private static void updateMaterializedView(Statement statement, String tableName, Map<String, String> columns, Map<String, String> newColumns) throws Exception {
        if (!newColumns.isEmpty()) {
            String mvName = tableName + "_mv";
            String dropMVQuery = "DROP VIEW IF EXISTS " + mvName;
            statement.execute(dropMVQuery);
            System.out.println("Materialized view dropped: " + mvName);
            createMaterializedView(statement, tableName, columns);
        }
    }

    private static void createKafkaTable(Statement statement, String tableName) throws Exception {
        String kafkaTableName = tableName + "_kafka";

        StringBuilder createKafkaTableQuery = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(kafkaTableName)
                .append(" (payload Nullable(String)) ")
                .append("ENGINE = Kafka() ")
                .append("SETTINGS kafka_broker_list = '192.168.1.17:9092', ")
                .append("kafka_topic_list = '").append(tableName).append("_topic', ")
                .append("kafka_group_name = '").append(tableName).append("_group', ")
                .append("kafka_format = 'JSONAsString', kafka_num_consumers = 1, ")
                .append("kafka_handle_error_mode = 'stream'");

        System.out.println("createKafkaTableQuery: " + createKafkaTableQuery);
        statement.execute(createKafkaTableQuery.toString());
        System.out.println("kfk table created: " + kafkaTableName);
    }

    private static void createMaterializedView(Statement statement, String tableName, Map<String, String> columns) throws SQLException {
        String kafkaTableName = tableName + "_kafka";
        String mvName = tableName + "_mv";
        StringBuilder selectQuery = new StringBuilder("SELECT ");

        int i = 0;
        for (Map.Entry<String, String> entry : columns.entrySet()) {
            if (i++ > 0) selectQuery.append(", ");
            selectQuery.append(generateJsonExtractExpression(entry.getKey()));
        }
        System.out.println("selectQuery: " + selectQuery);

        String createMVQuery = "CREATE MATERIALIZED VIEW IF NOT EXISTS " + mvName +
                " TO " + tableName +
                " AS " + selectQuery +
                " FROM " + kafkaTableName +
                " WHERE _error = ''";

        System.out.println("Create Materialized View query: " + createMVQuery);
        statement.execute(createMVQuery);
        System.out.println("Materialized view created: " + mvName);
    }

    private static void createErrorMaterializedView(Statement statement, String tableName) throws SQLException {
        String kafkaTableName = tableName + "_kafka";
        String errorMVName = tableName + "_errors_mv";
        String errorTableName = tableName + "_errors";

        String createErrorMVQuery = "CREATE MATERIALIZED VIEW IF NOT EXISTS " + errorMVName +
                " TO " + errorTableName +
                " AS SELECT " +
                "_raw_message AS payload, " +
                "_error AS error_message" +
                " FROM " + kafkaTableName +
                " WHERE _error != ''";

        System.out.println("Create Error Materialized View query: " + createErrorMVQuery);
        statement.execute(createErrorMVQuery);
        System.out.println("Error Materialized view created: " + errorMVName);
    }

    private static String generateJsonExtractExpression(String fullKey) {
        String[] parts = fullKey.split("__");

        if (fullKey.contains("__")) {
            StringBuilder expression = new StringBuilder("JSONExtractRaw(payload, '").append(parts[0]).append("')");

            for (int i = 1; i < parts.length - 1; i++) {
                expression = new StringBuilder("JSONExtractRaw(").append(expression).append(", '").append(parts[i]).append("')");
            }

            expression = new StringBuilder("JSONExtract(").append(expression).append(", '").append(parts[parts.length - 1])
                    .append("', 'Nullable(String)') AS ").append(fullKey);
            return expression.toString();
        } else {
            return "JSONExtract(payload, '" + fullKey + "', 'Nullable(String)') AS " + fullKey;
        }
    }
}
