package com.fuzzy.common.oracle;

import com.fuzzy.GlobalState;

import java.util.List;

public class CompositeTestOracle<G extends GlobalState<?, ?, ?>> implements TestOracle<G> {

    private final List<TestOracle<G>> oracles;
    private final G globalState;
    private int i;
    private int iLast;

    public CompositeTestOracle(List<TestOracle<G>> oracles, G globalState) {
        this.globalState = globalState;
        this.oracles = oracles;
    }

    @Override
    public void check() throws Exception {
        try {
            oracles.get(i).check();
            iLast = i;
            boolean lastOracleIndex = i == oracles.size() - 1;
            if (!lastOracleIndex) {
                globalState.getManager().incrementSelectQueryCount();
            }
        } finally {
            i = (i + 1) % oracles.size();
        }
    }

    @Override
    public String getLastQueryString() {
        return oracles.get(iLast).getLastQueryString();
    }
}
