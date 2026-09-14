package com.hcsc.bridge.mock;

import com.hcsc.bridge.ledger.LedgerEntry;
import com.hcsc.bridge.ledger.LedgerRepository;
import com.hcsc.bridge.ledger.LedgerState;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class InMemoryLedgerRepository implements LedgerRepository {

    private final Map<String, LedgerEntry> entries = new ConcurrentHashMap<>();

    @Override
    public void save(LedgerEntry entry) {
        entries.put(entry.getMessageId(), entry);
    }

    @Override
    public void update(LedgerEntry entry) {
        entries.put(entry.getMessageId(), entry);
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
    }

    public void clear() {
        entries.clear();
    }

    public List<LedgerEntry> getAll() {
        return new ArrayList<>(entries.values());
    }

    public int size() {
        return entries.size();
    }
}
