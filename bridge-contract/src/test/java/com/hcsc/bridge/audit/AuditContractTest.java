package com.hcsc.bridge.audit;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Ties the reconciliation scripts to the audit contract they read.
 *
 * <p>The balance and gap checks are shell and HQL: they name event types as string literals
 * and pull metadata out of {@code metadata_json} by key. Nothing in a normal build connects
 * those strings to {@link AuditEventType} and {@link AuditMetadata}, so renaming an event or
 * a metadata key used to break an equation silently - a reconciliation that reports PASS
 * because it is counting something that no longer exists. These tests fail the build instead.
 */
@DisplayName("audit contract vs the reconciliation scripts")
class AuditContractTest {

    private static final Path REPO = repoRoot();
    private static final List<String> SCRIPTS =
            List.of("scripts/abc-balance-check.sh", "scripts/audit-gap-check.sh");

    private static Path repoRoot() {
        // The test runs from the module directory; the scripts live at the reactor root.
        Path dir = Paths.get("").toAbsolutePath();
        while (dir != null && !Files.exists(dir.resolve("scripts/abc-balance-check.sh"))) {
            dir = dir.getParent();
        }
        return dir;
    }

    private String read(String script) throws IOException {
        return new String(Files.readAllBytes(REPO.resolve(script)), StandardCharsets.UTF_8);
    }

    @Test
    @DisplayName("every event type the scripts count exists in AuditEventType")
    void scriptsReferenceOnlyDeclaredEventTypes() throws IOException {
        Set<String> declared = java.util.Arrays.stream(AuditEventType.values())
                .map(Enum::name).collect(Collectors.toSet());
        // SCREAMING_SNAKE tokens inside single quotes are the event-type literals in the HQL
        Pattern literal = Pattern.compile("'([A-Z][A-Z_]{4,})'");
        List<String> unknown = new ArrayList<>();
        for (String script : SCRIPTS) {
            Matcher m = literal.matcher(read(script));
            while (m.find()) {
                String token = m.group(1);
                // Only tokens that look like our event types: the scripts also quote status
                // words (PASS, FAIL, INFO), error codes and column names.
                if (token.endsWith("_COMPLETED") || token.endsWith("_FAILED") || token.endsWith("_SKIPPED")
                        || token.startsWith("MESSAGE_") || token.startsWith("CLAIM_CHECK_")
                        || token.startsWith("HIVE_LOAD_") || token.startsWith("API_CALL_")) {
                    if (!declared.contains(token)) {
                        unknown.add(script + ": " + token);
                    }
                }
            }
        }
        assertThat(unknown)
                .as("event types named in the reconciliation scripts but absent from AuditEventType")
                .isEmpty();
    }

    @Test
    @DisplayName("every metadata key the scripts read is declared in AuditMetadata")
    void scriptsReadOnlyDeclaredMetadataKeys() throws IOException {
        Set<String> declared = declaredMetadataKeys();
        Pattern getJson = Pattern.compile("get_json_object\\([^,]+,\\s*'\\\\?\\$\\.([A-Za-z0-9_]+)'");
        Set<String> read = new LinkedHashSet<>();
        for (String script : SCRIPTS) {
            Matcher m = getJson.matcher(read(script));
            while (m.find()) {
                read.add(m.group(1));
            }
        }
        assertThat(read).as("the scripts should read at least the pipeline and errorCode keys")
                .contains(AuditMetadata.PIPELINE, AuditMetadata.ERROR_CODE);
        assertThat(read).as("metadata keys read by the reconciliation scripts").isSubsetOf(declared);
    }

    @Test
    @DisplayName("AuditMetadata declares no duplicate key names")
    void metadataKeysAreUnique() {
        List<String> keys = new ArrayList<>(declaredMetadataKeys());
        assertThat(keys).doesNotHaveDuplicates();
    }

    private Set<String> declaredMetadataKeys() {
        Set<String> keys = new LinkedHashSet<>();
        for (java.lang.reflect.Field f : AuditMetadata.class.getDeclaredFields()) {
            if (f.getType() == String.class && java.lang.reflect.Modifier.isStatic(f.getModifiers())) {
                try {
                    keys.add((String) f.get(null));
                } catch (IllegalAccessException e) {
                    throw new AssertionError(e);
                }
            }
        }
        return keys;
    }
}
