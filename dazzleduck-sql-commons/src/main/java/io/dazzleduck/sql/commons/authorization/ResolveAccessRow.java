package io.dazzleduck.sql.commons.authorization;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.List;

/**
 * Represents a single access row returned by the redirect resolve endpoint.
 * Maps to entries in the "tables" or "functions" arrays of the resolve response.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record ResolveAccessRow(
        Long id,
        String clusterName,
        String sourceType,
        String sourceName,
        String database,
        String schema,
        String tableOrPath,
        String tableType,
        List<String> columns,
        String filter,
        String functionName,
        String expiration,
        String accessTime) {
}
