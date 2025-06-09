package com.benchmark.influxdb.hint;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FluxHintInjector {

    private static final Pattern SLIMIT_PATTERN = Pattern.compile(
            "^SLIMIT\\s+(\\d+)(?:\\s+SOFFSET\\s+(\\d+))?$",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern TOP_PATTERN = Pattern.compile(
            "^TOP\\((\\d+)\\)$",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern BOTTOM_PATTERN = Pattern.compile(
            "^BOTTOM\\((\\d+)\\)$",
            Pattern.CASE_INSENSITIVE
    );

    private static final Pattern FILL_PATTERN = Pattern.compile(
            "^FILL\\((previous|linear|none|\\d+\\.?\\d*)\\)$",
            Pattern.CASE_INSENSITIVE
    );

    /**
     * Injects a limit() into baseFlux according to a SLIMIT hint.
     * - If hint is null/blank or doesn’t start with “SLIMIT”, returns baseFlux unchanged.
     * - If hint starts with “SLIMIT” but fails to parse, throws IllegalArgumentException.
     */

    public static String applyHint(String baseQuery, String hint) {
        if (baseQuery == null || hint == null || hint.isEmpty()) {
            return baseQuery;
        }
        String trimmed = baseQuery.trim();

        if (trimmed.toUpperCase(Locale.ROOT).startsWith("SELECT")
                && !trimmed.contains("|>")) {
            return baseQuery + " " + hint;
        }

        return baseQuery;
    }


    private static String processSLIMIT(String baseFlux, Matcher matcher) {
        try {
            int n = Integer.parseInt(matcher.group(1));
            int offset = (matcher.group(2) != null)
                    ? Integer.parseInt(matcher.group(2))
                    : 0;

            String cleaned = baseFlux.replaceAll(
                    "(?i)\\|>\\s*limit\\s*\\([^)]*\\)",
                    ""
            ).trim();

            return cleaned + "\n  |> limit(n:" + n + ", offset:" + offset + ")";
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid number in SLIMIT hint: " + matcher.group(0), ex);
        }
    }

    private static String processTOP(String baseFlux, Matcher matcher) {
        try {
            int n = Integer.parseInt(matcher.group(1));

            // Remove existing top()
            String cleaned = baseFlux.replaceAll(
                    "(?i)\\|>\\s*top\\s*\\([^)]*\\)",
                    ""
            ).trim();

            return cleaned + "\n  |> top(n:" + n + ")";
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid number in TOP hint: " + matcher.group(0), ex);
        }
    }

    private static String processBOTTOM(String baseFlux, Matcher matcher) {
        try {
            int n = Integer.parseInt(matcher.group(1));

            // Remove existing bottom()
            String cleaned = baseFlux.replaceAll(
                    "(?i)\\|>\\s*bottom\\s*\\([^)]*\\)",
                    ""
            ).trim();

            return cleaned + "\n  |> bottom(n:" + n + ")";
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException("Invalid number in BOTTOM hint: " + matcher.group(0), ex);
        }
    }

    private static String processFILL(String baseFlux, Matcher matcher) {
        // only FILL(previous) is supported
        String cleaned = baseFlux.replaceAll(
                "(?i)\\|>\\s*fill\\s*\\([^)]*\\)",
                ""
        ).trim();
        return cleaned + "\n  |> fill(usePrevious: true)";
    }


}
