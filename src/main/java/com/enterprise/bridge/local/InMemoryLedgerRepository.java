package com.enterprise.bridge.local;

import com.enterprise.bridge.ledger.LedgerEntry;
import com.enterprise.bridge.ledger.LedgerRepository;
import com.enterprise.bridge.ledger.LedgerState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Repository
@Profile("local")
public class InMemoryLedgerRepository implements LedgerRepository {

    private static final Logger logger = LoggerFactory.getLogger(InMemoryLedgerRepository.class);

    private final Map<String, LedgerEntry> entries = new ConcurrentHashMap<>();

    public InMemoryLedgerRepository() {
        logger.info("Initialized in-memory ledger repository for local development");
    }

    @Override
    public void save(LedgerEntry entry) {
        entries.put(entry.getMessageId(), entry);
        logger.debug("Saved ledger entry: {}", entry.getMessageId());
    }

    @Override
    public void update(LedgerEntry entry) {
        entries.put(entry.getMessageId(), entry);
        logger.debug("Updated ledger entry: {} -> {}", entry.getMessageId(), entry.getState());
    }

    @Override
    public Optional<LedgerEntry> findByMessageId(String messageId) {
        return Optional.ofNullable(entries.get(messageId));
    }

    @Override
    public List<LedgerEntry> findByState(LedgerState state) {
        return entries.values().stream()
                .filter(e -> e.getState() == state)
                .collect(Collectors.toList());
    }

    @Override
    public List<LedgerEntry> findByStates(List<LedgerState> states) {
        return entries.values().stream()
                .filter(e -> states.contains(e.getState()))
                .collect(Collectors.toList());
    }

    @Override
    public List<LedgerEntry> findRecoverableEntries() {
        return entries.values().stream()
                .filter(e -> e.getState().isRecoverable())
                .collect(Collectors.toList());
    }

    @Override
    public boolean existsByMessageId(String messageId) {
        return entries.containsKey(messageId);
    }

    @Override
    public long countByState(LedgerState state) {
        return entries.values().stream()
                .filter(e -> e.getState() == state)
                .count();
    }

    @Override
    public void delete(String messageId) {
        entries.remove(messageId);
        logger.debug("Deleted ledger entry: {}", messageId);
    }
}
