package com.tdsecurities.interview_question;

import org.springframework.batch.item.ItemProcessor;
import java.util.*;

/**
 * <h1>TradeItemProcessor</h1>
 * Build a TradeItemProcessor to process each one record of Trade
 * and split it to multiple record of Trades if possible
 * <p>
 *
 */

public class TradeItemProcessor implements ItemProcessor<Trade, List<Trade>> {
    /**
     * Initialize the necessary variables for the methods to use
     */
    private HashMap<String, int[]> dict = new HashMap<>();
    private static List<String> termYear = new ArrayList<>();
    private static List<Integer> termValue = new ArrayList<>();

    /**
     * Store the term bucket in dict
     * Key Value Pair: Key = Term in date range, Value: Array of start date and end date
     */
    public void setDict() {
        this.dict.put("3M", new int[]{0, 90});
        this.dict.put("6M", new int[]{91, 180});
        this.dict.put("1Y", new int[]{181, 365});
        this.dict.put("2Y", new int[]{366, 730});
        this.dict.put("5Y", new int[]{731, 1825});
        this.dict.put("10Y", new int[]{1826, 3650});
    }

    /**
     * For term over 10Y, split it into multiple Strings and add to termYear
     * For tradeValue, calculate the according portion of it and store in termValue
     * @param currTerm the term after deducting corresponding term bucket end date
     * @param allTerm the term remain the same as the denominator when calculate the trade value
     * @param tradeValue the original trade value to be split into portions
     */
    public void calcTermValue(int currTerm, int allTerm, int tradeValue) {
        if (currTerm < 0) {
            return;
        } else if (currTerm <= 3650) {
            termValue.add(Math.round((((float) currTerm/allTerm * tradeValue))));
            for (Map.Entry<String, int[]> entry: this.dict.entrySet()) {
                if (currTerm >= entry.getValue()[0] && currTerm <= entry.getValue()[1]) {
                    termYear.add(entry.getKey());
                }
            }
            return;
        } else {
            currTerm = currTerm - 3650;
            termYear.add("10Y");
            termValue.add(Math.round((((float) 3650/allTerm) * tradeValue)));
            this.calcTermValue(currTerm, allTerm, tradeValue);
        }
    }

    /**
     * Override the process and output the list of objects
     * @param trade Trade Object to be processed
     * @return List<Trade>
     */
    @Override
    public List<Trade> process(final Trade trade) {
        List<Trade> tradeList = new ArrayList<>();
        termYear = new ArrayList<>();
        termValue = new ArrayList<>();


        setDict();
        String tradeId = trade.getTradeId();
        int currTerm = Integer.parseInt(trade.getTerm());
        int allTerm = Integer.parseInt(trade.getTerm());
        int tradeValue = Integer.parseInt(trade.getTradeValue());
        String currency = trade.getCurrency();
        calcTermValue(currTerm, allTerm, tradeValue);

        for (int i = 0; i < termYear.size(); i++) {
            Trade t = new Trade(tradeId, termYear.get(i), termValue.get(i).toString(), currency);
            tradeList.add(t);
        }

        return tradeList;
    }
}
