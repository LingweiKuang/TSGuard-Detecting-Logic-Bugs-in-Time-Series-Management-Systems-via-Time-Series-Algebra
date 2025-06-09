package com.benchmark.influxdb.hint;

import java.util.Random;

/**
 * Generates random InfluxDB hint clauses for fuzz testing.
 */
public class HintGenerator {
    //hint templates
    private static final String[] TEMPLATES = {
            "SLIMIT %d",
            "SLIMIT %d SOFFSET %d",
            "FILL(previous)",
            "OFFSET %d",
//            "TOP(%d)",
//            "BOTTOM(%d)"
    };

    private final Random random = new Random();
    // Maximum values for numeric hints
    private final int maxLimit = 30;
    private final int maxOffset = 10;

    /**
     * Returns a randomly chosen hint, formatted with random parameters where needed.
     */
    public String nextHint() {
        String template = TEMPLATES[random.nextInt(TEMPLATES.length)];
        if (template.contains("%d")){
            if (template.contains("SOFFSET")){
                int limit = randInt(1, maxLimit);
                int offset = randInt(0, maxOffset);
                return String.format(template, limit, offset);
            } else {
                int value = randInt(1, maxLimit);
                return String.format(template, value);
            }
        }
        return template;

    }



    private int randInt(int min, int max) {
        return random.nextInt(max - min + 1) + min;
    }
}
