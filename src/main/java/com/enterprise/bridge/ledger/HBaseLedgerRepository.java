package com.enterprise.bridge.ledger;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Repository
@Profile("hbase")
public class HBaseLedgerRepository implements LedgerRepository {

    private static final Logger logger = LoggerFactory.getLogger(HBaseLedgerRepository.class);

    private static final byte[] COLUMN_FAMILY = Bytes.toBytes("cf");
    private static final byte[] COL_MESSAGE_ID = Bytes.toBytes("bridge_message_id");
    private static final byte[] COL_TRANSACTION_ID = Bytes.toBytes("original_mq_message_id");
    private static final byte[] COL_STATUS = Bytes.toBytes("status");
    private static final byte[] COL_HDFS_PATH = Bytes.toBytes("hdfs_path");
    private static final byte[] COL_CHECKSUM = Bytes.toBytes("checksum");
    private static final byte[] COL_KAFKA_TOPIC = Bytes.toBytes("kafka_topic");
    private static final byte[] COL_KAFKA_PARTITION = Bytes.toBytes("kafka_partition");
    private static final byte[] COL_KAFKA_OFFSET = Bytes.toBytes("kafka_offset");
    private static final byte[] COL_ERROR_CODE = Bytes.toBytes("error_code");
    private static final byte[] COL_ERROR_MESSAGE = Bytes.toBytes("error_message");
    private static final byte[] COL_RETRY_COUNT = Bytes.toBytes("retry_count");
    private static final byte[] COL_CREATED_AT = Bytes.toBytes("created_at_utc");
    private static final byte[] COL_UPDATED_AT = Bytes.toBytes("updated_at_utc");
    private static final byte[] COL_MARKETING_PLAN_ID = Bytes.toBytes("marketing_plan_id");
    private static final byte[] COL_EFFECTIVE_DATE = Bytes.toBytes("effective_date");

    private final Connection connection;
    private final TableName tableName;

    public HBaseLedgerRepository(
            Connection connection,
            @Value("${bridge.hbase.ledger-table:bridge_ledger}") String tableName) {
        this.connection = connection;
        this.tableName = TableName.valueOf(tableName);
        logger.info("Initialized HBase ledger repository with table: {}", tableName);
    }

    @Override
    public void save(LedgerEntry entry) {
        try (Table table = connection.getTable(tableName)) {
            Put put = createPut(entry);
            table.put(put);
            logger.debug("Saved ledger entry to HBase: {}", entry.getMessageId());
        } catch (IOException e) {
            logger.error("Failed to save ledger entry: {}", entry.getMessageId(), e);
            throw new LedgerPersistenceException("Failed to save ledger entry", e);
        }
    }

    @Override
    public void update(LedgerEntry entry) {
        save(entry);
    }

    @Override
    public Optional<LedgerEntry> findByMessageId(String messageId) {
        try (Table table = connection.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(messageId));
            get.addFamily(COLUMN_FAMILY);
            Result result = table.get(get);
            if (result.isEmpty()) {
                return Optional.empty();
            }
            return Optional.of(resultToEntry(result));
        } catch (IOException e) {
            logger.error("Failed to find ledger entry: {}", messageId, e);
            throw new LedgerPersistenceException("Failed to find ledger entry", e);
        }
    }

    @Override
    public List<LedgerEntry> findByState(LedgerState state) {
        return scanWithStateFilter(state.name());
    }

    @Override
    public List<LedgerEntry> findByStates(List<LedgerState> states) {
        List<LedgerEntry> results = new ArrayList<>();
        for (LedgerState state : states) {
            results.addAll(findByState(state));
        }
        return results;
    }

    @Override
    public List<LedgerEntry> findRecoverableEntries() {
        List<LedgerEntry> results = new ArrayList<>();
        results.addAll(findByState(LedgerState.FAILED_ENRICHMENT));
        results.addAll(findByState(LedgerState.FAILED_HDFS));
        results.addAll(findByState(LedgerState.FAILED_KAFKA));
        results.addAll(findByState(LedgerState.RECOVERY_PENDING));
        return results;
    }

    @Override
    public boolean existsByMessageId(String messageId) {
        try (Table table = connection.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(messageId));
            return table.exists(get);
        } catch (IOException e) {
            logger.error("Failed to check existence: {}", messageId, e);
            throw new LedgerPersistenceException("Failed to check ledger entry existence", e);
        }
    }

    @Override
    public long countByState(LedgerState state) {
        return findByState(state).size();
    }

    @Override
    public void delete(String messageId) {
        try (Table table = connection.getTable(tableName)) {
            Delete delete = new Delete(Bytes.toBytes(messageId));
            table.delete(delete);
            logger.debug("Deleted ledger entry from HBase: {}", messageId);
        } catch (IOException e) {
            logger.error("Failed to delete ledger entry: {}", messageId, e);
            throw new LedgerPersistenceException("Failed to delete ledger entry", e);
        }
    }

    private Put createPut(LedgerEntry entry) {
        byte[] rowKey = Bytes.toBytes(entry.getMessageId());
        Put put = new Put(rowKey);

        put.addColumn(COLUMN_FAMILY, COL_MESSAGE_ID, toBytes(entry.getMessageId()));
        put.addColumn(COLUMN_FAMILY, COL_TRANSACTION_ID, toBytes(entry.getTransactionId()));
        put.addColumn(COLUMN_FAMILY, COL_STATUS, toBytes(entry.getState().name()));
        put.addColumn(COLUMN_FAMILY, COL_HDFS_PATH, toBytes(entry.getHdfsPath()));
        put.addColumn(COLUMN_FAMILY, COL_CHECKSUM, toBytes(entry.getChecksum()));
        put.addColumn(COLUMN_FAMILY, COL_KAFKA_OFFSET, toBytes(entry.getKafkaOffset()));
        put.addColumn(COLUMN_FAMILY, COL_ERROR_MESSAGE, toBytes(entry.getLastError()));
        put.addColumn(COLUMN_FAMILY, COL_RETRY_COUNT, Bytes.toBytes(entry.getRetryCount()));
        put.addColumn(COLUMN_FAMILY, COL_CREATED_AT, Bytes.toBytes(entry.getCreatedAt().toEpochMilli()));
        put.addColumn(COLUMN_FAMILY, COL_UPDATED_AT, Bytes.toBytes(entry.getUpdatedAt().toEpochMilli()));

        return put;
    }

    private LedgerEntry resultToEntry(Result result) {
        byte[] rowKey = result.getRow();
        String messageId = Bytes.toString(rowKey);

        return LedgerEntry.builder()
                .messageId(messageId)
                .transactionId(getString(result, COL_TRANSACTION_ID))
                .state(LedgerState.valueOf(getString(result, COL_STATUS)))
                .hdfsPath(getString(result, COL_HDFS_PATH))
                .checksum(getString(result, COL_CHECKSUM))
                .kafkaOffset(getString(result, COL_KAFKA_OFFSET))
                .lastError(getString(result, COL_ERROR_MESSAGE))
                .retryCount(getInt(result, COL_RETRY_COUNT))
                .createdAt(getInstant(result, COL_CREATED_AT))
                .updatedAt(getInstant(result, COL_UPDATED_AT))
                .build();
    }

    private List<LedgerEntry> scanWithStateFilter(String state) {
        List<LedgerEntry> entries = new ArrayList<>();
        try (Table table = connection.getTable(tableName)) {
            Scan scan = new Scan();
            scan.addFamily(COLUMN_FAMILY);
            SingleColumnValueFilter filter = new SingleColumnValueFilter(
                    COLUMN_FAMILY,
                    COL_STATUS,
                    CompareFilter.CompareOp.EQUAL,
                    Bytes.toBytes(state)
            );
            filter.setFilterIfMissing(true);
            scan.setFilter(filter);

            try (ResultScanner scanner = table.getScanner(scan)) {
                for (Result result : scanner) {
                    entries.add(resultToEntry(result));
                }
            }
        } catch (IOException e) {
            logger.error("Failed to scan for state: {}", state, e);
            throw new LedgerPersistenceException("Failed to scan ledger entries", e);
        }
        return entries;
    }

    private byte[] toBytes(String value) {
        return value != null ? value.getBytes(StandardCharsets.UTF_8) : new byte[0];
    }

    private String getString(Result result, byte[] column) {
        byte[] value = result.getValue(COLUMN_FAMILY, column);
        return value != null && value.length > 0 ? new String(value, StandardCharsets.UTF_8) : null;
    }

    private int getInt(Result result, byte[] column) {
        byte[] value = result.getValue(COLUMN_FAMILY, column);
        return value != null && value.length > 0 ? Bytes.toInt(value) : 0;
    }

    private Instant getInstant(Result result, byte[] column) {
        byte[] value = result.getValue(COLUMN_FAMILY, column);
        if (value != null && value.length > 0) {
            return Instant.ofEpochMilli(Bytes.toLong(value));
        }
        return Instant.now();
    }

    public static class LedgerPersistenceException extends RuntimeException {
        public LedgerPersistenceException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
