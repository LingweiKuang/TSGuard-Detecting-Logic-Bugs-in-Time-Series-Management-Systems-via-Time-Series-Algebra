package com.benchmark.influxdb.hint;

import com.benchmark.entity.DBVal;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Compares two lists of DBVal rows and identifies mismatches.
 */
public class ResultComparator {

    /**
     * Compares expected and actual lists of DBVal and returns a list of mismatch descriptions.
     * If the lists are identical, the returned list is empty.
     */
    public static List<String> compare(List<DBVal> expected, List<DBVal> actual) {
        List<String> mismatches = new ArrayList<>();
        // both null → no mismatches
        if (expected == null && actual == null) {
            return mismatches;
        }
        // one null → bail out
        if (expected == null || actual == null) {
            mismatches.add("One or result set is null, while other is not null");
            return mismatches;
        }

        int expSize = expected.size();
        int actSize = actual.size();
        // size mismatch
        if (expSize != actSize) {
            mismatches.add(String.format(
                    "Size mismatch: expected %d rows but got %d rows.",
                    expSize, actSize
            ));
        }

        // compare row-by-row up to the smaller of the two sizes
        int common = Math.min(expSize, actSize);
        for (int i = 0; i < common; i++) {
            DBVal e = expected.get(i);
            DBVal a = actual.get(i);

            // table name
            if (!Objects.equals(e.getTableName(), a.getTableName())) {
                mismatches.add(String.format(
                        "Row %d: Table Name: Expected '%s' vs Actual '%s'",
                        i, e.getTableName(), a.getTableName()
                ));
            }
            // tag name
            if (!Objects.equals(e.getTagName(), a.getTagName())) {
                mismatches.add(String.format(
                        "Row %d: Tag Name: Expected '%s' vs Actual '%s'",
                        i, e.getTagName(), a.getTagName()
                ));
            }
            // tag value
            if (!Objects.equals(e.getTagValue(), a.getTagValue())) {
                mismatches.add(String.format(
                        "Row %d: Tag Value: Expected '%s' vs Actual '%s'",
                        i, e.getTagValue(), a.getTagValue()
                ));
            }
            // UTC time (in seconds)
            if (e.getUtcTime() != a.getUtcTime()) {
                mismatches.add(String.format(
                        "Row %d: UTC Time: Expected '%d' vs Actual '%d'",
                        i, e.getUtcTime(), a.getUtcTime()
                ));
            }
            // field-value lists
            List<Object> ev = e.getFieldValues();
            List<Object> av = a.getFieldValues();
            if (!Objects.equals(ev, av)) {
                mismatches.add(String.format(
                        "Row %d: Field values differ: Expected %s vs Actual %s",
                        i, ev, av
                ));
            }
        }

        // any unexpected extra rows in actual?
        if (actSize > expSize) {
            for (int i = expSize; i < actSize; i++) {
                mismatches.add(String.format(
                        "Unexpected extra row %d: %s",
                        i, actual.get(i).toString()
                ));
            }
        }
        // any missing rows from actual?
        if (expSize > actSize) {
            for (int i = actSize; i < expSize; i++) {
                mismatches.add(String.format(
                        "Missing expected row %d: %s",
                        i, expected.get(i).toString()
                ));
            }
        }

        return mismatches;
    }
}
