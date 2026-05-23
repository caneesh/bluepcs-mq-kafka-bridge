package com.enterprise.bridge.ledger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Repository
@Profile("file-ledger")
public class FileLedgerRepository implements LedgerRepository {

    private static final Logger logger = LoggerFactory.getLogger(FileLedgerRepository.class);

    private final Path storagePath;
    private final ObjectMapper objectMapper;
    private final Map<String, LedgerEntry> cache;

    public FileLedgerRepository(@Value("${bridge.ledger.storage-path:/tmp/ledger}") String storagePath) {
        this.storagePath = Paths.get(storagePath);
        this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.cache = new ConcurrentHashMap<>();
    }

    @PostConstruct
    public void init() {
        try {
            Files.createDirectories(storagePath);
            loadExistingEntries();
        } catch (IOException e) {
            logger.error("Failed to initialize ledger storage", e);
            throw new RuntimeException("Failed to initialize ledger storage", e);
        }
    }

    private void loadExistingEntries() {
        try {
            if (Files.exists(storagePath)) {
                Files.list(storagePath)
                        .filter(p -> p.toString().endsWith(".json") && !p.toString().endsWith(".tmp"))
                        .forEach(this::loadEntry);
                cleanupOrphanedTempFiles();
            }
        } catch (IOException e) {
            logger.error("Failed to load existing ledger entries", e);
        }
    }

    private void cleanupOrphanedTempFiles() {
        try {
            Files.list(storagePath)
                    .filter(p -> p.toString().endsWith(".json.tmp"))
                    .forEach(p -> {
                        try {
                            Files.deleteIfExists(p);
                            logger.warn("Cleaned up orphaned temp file: {}", p);
                        } catch (IOException e) {
                            logger.error("Failed to cleanup temp file: {}", p, e);
                        }
                    });
        } catch (IOException e) {
            logger.error("Failed to cleanup orphaned temp files", e);
        }
    }

    private void loadEntry(Path path) {
        try {
            LedgerEntry entry = objectMapper.readValue(path.toFile(), LedgerEntry.class);
            cache.put(entry.getMessageId(), entry);
        } catch (IOException e) {
            logger.error("Failed to load ledger entry: {}", path, e);
        }
    }

    @Override
    public void save(LedgerEntry entry) {
        persistEntry(entry);
        cache.put(entry.getMessageId(), entry);
    }

    @Override
    public void update(LedgerEntry entry) {
        persistEntry(entry);
        cache.put(entry.getMessageId(), entry);
    }

    private void persistEntry(LedgerEntry entry) {
        try {
            Path entryPath = storagePath.resolve(entry.getMessageId() + ".json");
            Path tempPath = storagePath.resolve(entry.getMessageId() + ".json.tmp");
            String json = objectMapper.writeValueAsString(entry);
            Files.writeString(tempPath, json,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING);
            Files.move(tempPath, entryPath,
                    StandardCopyOption.ATOMIC_MOVE,
                    StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            logger.error("Failed to persist ledger entry: {}", entry.getMessageId(), e);
            throw new RuntimeException("Failed to persist ledger entry", e);
        }
    }

    @Override
    public Optional<LedgerEntry> findByMessageId(String messageId) {
        return Optional.ofNullable(cache.get(messageId));
    }

    @Override
    public List<LedgerEntry> findByState(LedgerState state) {
        return cache.values().stream()
                .filter(e -> e.getState() == state)
                .collect(Collectors.toList());
    }

    @Override
    public List<LedgerEntry> findByStates(List<LedgerState> states) {
        return cache.values().stream()
                .filter(e -> states.contains(e.getState()))
                .collect(Collectors.toList());
    }

    @Override
    public List<LedgerEntry> findRecoverableEntries() {
        return cache.values().stream()
                .filter(e -> e.getState().isRecoverable())
                .collect(Collectors.toList());
    }

    @Override
    public boolean existsByMessageId(String messageId) {
        return cache.containsKey(messageId);
    }

    @Override
    public long countByState(LedgerState state) {
        return cache.values().stream()
                .filter(e -> e.getState() == state)
                .count();
    }

    @Override
    public void delete(String messageId) {
        try {
            Path entryPath = storagePath.resolve(messageId + ".json");
            Files.deleteIfExists(entryPath);
            cache.remove(messageId);
        } catch (IOException e) {
            logger.error("Failed to delete ledger entry file: {}", messageId, e);
            throw new RuntimeException("Failed to delete ledger entry", e);
        }
    }
}
