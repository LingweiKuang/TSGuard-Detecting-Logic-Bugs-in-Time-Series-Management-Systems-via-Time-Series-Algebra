package com.benchmark.influxdb.hint;

import com.benchmark.entity.DBVal;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Applies InfluxDB hint semantics to a baseline list of DBVal and returns the expected list.
 */
public class ExpectedResultGenerator {

    /**
     * Dispatch based on hint type.
     * @param baseline the original DBVal list from an un-hinted query
     * @param hint     the hint clause (e.g., "FILL(previous)")
     * @return the expected DBVal list after applying hint semantics
     */
    public static List<DBVal> apply(List<DBVal> baseline, String hint) {
        if (hint == null || hint.isEmpty()) {
            return baseline;
        }
        String h = hint.trim().toUpperCase();

        if (h.startsWith("FILL")) {
            return applyFillPrevious(baseline);
        } else if (h.startsWith("SLIMIT")) {
            return applySLimit(baseline, h);
        } else if(h.startsWith("TOP")){
            return applyTop(baseline, h);
        } else if(h.startsWith("BOTTOM")){
            return applyBottom(baseline, h);
        }
        return baseline;
    }

    private static List<DBVal> applyFillPrevious(List<DBVal> rows) {
        List<DBVal> out = new ArrayList<>();
        List<Object> lastSeen = null;
        for (DBVal r : rows) {
            List<Object> values = new ArrayList<>(r.getFieldValues());
            boolean hasNull = values.stream().anyMatch(Objects::isNull);
            if (!hasNull) {
                lastSeen = new ArrayList<>(values);
                out.add(r);
            } else {
                if (lastSeen == null) {
                    out.add(r);
                    continue;
                }
                // fill nulls with last seen values
                List<Object> filled = new ArrayList<>();
                for (int i = 0; i < values.size(); i++) {
                    filled.add(values.get(i) != null ? values.get(i) : lastSeen.get(i));
                }
                // clone DBVal with new values
                DBVal clone = DBVal.DBValBuilder.anCommonDBVal(r)
                        .withFieldValues(filled)
                        .build();
                out.add(clone);
            }
        }
        return out;
    }

    private static List<DBVal> applySLimit(List<DBVal> rows, String hint) {
        // parse SLIMIT n [SOFFSET m]
        String[] parts = hint.replace("SLIMIT ", "").split(" SOFFSET ");
        int limit = Integer.parseInt(parts[0].trim());
        int offset = parts.length > 1 ? Integer.parseInt(parts[1].trim()) : 0;
        List<DBVal> out = new ArrayList<>();
        for (int i = offset; i < Math.min(rows.size(), offset + limit); i++) {
            out.add(rows.get(i));
        }
        return out;
    }

    private static List<DBVal> applyTop(List<DBVal> rows, String hint) {
        int n = Integer.parseInt(hint.substring(hint.indexOf('(') + 1, hint.indexOf(')')).trim());
        return rows.stream()
                .sorted(Comparator.comparingDouble(ExpectedResultGenerator::getFirstFieldValue).reversed())
                .limit(n)
                .collect(Collectors.toList());
    }

    private static List<DBVal> applyBottom(List<DBVal> rows, String hint) {
        int n = Integer.parseInt(hint.substring(hint.indexOf('(') + 1, hint.indexOf(')')).trim());
        return rows.stream()
                .sorted(Comparator.comparingDouble(ExpectedResultGenerator::getFirstFieldValue))
                .limit(n)
                .collect(Collectors.toList());
    }

    /**
     * Extracts the first field's numeric value from a DBVal, defaulting to 0 if unparsable.
     */
    private static double getFirstFieldValue(DBVal r) {
        Object o = r.getFieldValues().get(0);
        if (o instanceof Number) {
            return ((Number) o).doubleValue();
        }
        try {
            return Double.parseDouble(o.toString());
        } catch (Exception e) {
            return 0.0;
        }
    }
}
