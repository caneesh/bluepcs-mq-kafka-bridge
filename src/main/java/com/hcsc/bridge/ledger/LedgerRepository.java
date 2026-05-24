package com.hcsc.bridge.ledger;

import java.util.List;
import java.util.Optional;

public interface LedgerRepository {

    void save(LedgerEntry entry);

    void update(LedgerEntry entry);

    Optional<LedgerEntry> findByMessageId(String messageId);

    List<LedgerEntry> findByState(LedgerState state);

    List<LedgerEntry> findByStates(List<LedgerState> states);

    List<LedgerEntry> findRecoverableEntries();

    boolean existsByMessageId(String messageId);

    long countByState(LedgerState state);

    void delete(String messageId);
}
