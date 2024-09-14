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
import java.sql.Statement;
import java.util.*;

@Slf4j
@SpringBootApplication
public class ClickhouseKafkaConsumerApplication implements CommandLineRunner {
    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://192.168.1.13:18123/zhuravishkin";
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
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode rootNode = objectMapper.readTree(json);
            String tableName = rootNode.get("token").asText();
            System.out.println("payload: " + rootNode);

            ClickHouseDataSource dataSource = new ClickHouseDataSource(CLICKHOUSE_URL, new Properties());
            try (Connection conn = dataSource.getConnection("username", "password");
                 Statement stmt = conn.createStatement()) {
                boolean tableExists = isTableExist(stmt, tableName);

                if (tableExists) {
                    addMissingColumns(stmt, tableName, rootNode);
                } else {
                    createTable(stmt, tableName, rootNode);
                    createKafkaConsumer(stmt, tableName, rootNode);
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

    private static void createTable(Statement statement, String tableName, JsonNode fieldsNode) throws Exception {
        StringBuilder createTableQuery = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(tableName).append(" (");

        Iterator<Map.Entry<String, JsonNode>> fields = fieldsNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String columnName = field.getKey();

            createTableQuery.append(columnName).append(" ").append(COLUMN_TYPE).append(", ");
        }

        createTableQuery.append("_timestamp DateTime DEFAULT now()) ENGINE = MergeTree() ORDER BY _timestamp");
        System.out.println(createTableQuery);

        statement.execute(createTableQuery.toString());
        System.out.println("table created: " + tableName);
    }

    private static void addMissingColumns(Statement statement, String tableName, JsonNode fieldsNode) throws Exception {
        boolean newColumnsAdded = false;
        String query = "DESCRIBE TABLE " + tableName;
        ResultSet resultSet = statement.executeQuery(query);

        Set<String> existingColumns = new HashSet<>();
        while (resultSet.next()) {
            String column = resultSet.getString(1);
            System.out.println("column: " + column);
            existingColumns.add(column);
        }

        Iterator<Map.Entry<String, JsonNode>> fields = fieldsNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String columnName = field.getKey();

            if (!existingColumns.contains(columnName)) {
                String alterTableQuery = "ALTER TABLE " + tableName + " ADD COLUMN " + columnName + " " + COLUMN_TYPE;
                statement.execute(alterTableQuery);
                System.out.println("added column: " + columnName);

                newColumnsAdded = true;
            }
        }

        if (newColumnsAdded) {
            String dropMVQuery = "DROP VIEW IF EXISTS " + tableName + "_kafka_mv";
            statement.execute(dropMVQuery);
            System.out.println("mv dropped: " + tableName + "_kafka_mv");

            String dropKafkaTableQuery = "DROP TABLE IF EXISTS " + tableName + "_kafka";
            statement.execute(dropKafkaTableQuery);
            System.out.println("kfk table dropped: " + tableName + "_kafka");

            createKafkaConsumer(statement, tableName, fieldsNode);
        }
    }

    private static void createKafkaConsumer(Statement statement, String tableName, JsonNode fieldsNode) throws Exception {
        StringBuilder createKafkaTableQuery = new StringBuilder("CREATE TABLE IF NOT EXISTS ").append(tableName).append("_kafka (");

        Iterator<Map.Entry<String, JsonNode>> fields = fieldsNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String columnName = field.getKey();

            createKafkaTableQuery.append(columnName).append(" ").append(COLUMN_TYPE).append(", ");
        }

        createKafkaTableQuery.append(") ENGINE = Kafka() ")
                .append("SETTINGS kafka_broker_list = '192.168.56.101:9092', ")
                .append("kafka_topic_list = '").append(tableName).append("_topic', ")
                .append("kafka_group_name = '").append(tableName).append("_group', ")
                .append("kafka_format = 'JSONEachRow', kafka_num_consumers = 1");

        System.out.println("createKafkaTableQuery: " + createKafkaTableQuery);
        statement.execute(createKafkaTableQuery.toString());
        System.out.println("kfk table created: " + tableName + "_kafka");

        String createMaterializedViewQuery = "CREATE MATERIALIZED VIEW IF NOT EXISTS " + tableName + "_kafka_mv "
                + "TO " + tableName + " AS "
                + "SELECT * FROM " + tableName + "_kafka";

        statement.execute(createMaterializedViewQuery);
        System.out.println("mv created: " + tableName + "_kafka_mv");
    }
}
