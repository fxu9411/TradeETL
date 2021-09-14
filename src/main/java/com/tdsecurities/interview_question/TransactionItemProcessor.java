package com.tdsecurities.interview_question;

import org.springframework.batch.item.ItemProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.TransactionStatus;

import java.util.*;

public class TransactionItemProcessor implements ItemProcessor<Transaction, List<Transaction>> {
//    private static final Logger log = LoggerFactory.getLogger(TransactionItemProcessor.class);
    private HashMap<String, int[]> dict = new HashMap<>();
    private static List<String> termYear = new ArrayList<String>();
    private static List<Integer> termValue = new ArrayList<Integer>();

    public void setDict() {
        this.dict.put("3M", new int[]{0, 90});
        this.dict.put("6M", new int[]{91, 180});
        this.dict.put("1Y", new int[]{181, 365});
        this.dict.put("2Y", new int[]{366, 730});
        this.dict.put("5Y", new int[]{731, 1825});
        this.dict.put("10Y", new int[]{1826, 3650});
    }

    public void calcTermValue(int currTerm, int allTerm, int tradeValue) {
        if (currTerm < 0) {
            return;
        } else if (currTerm <= 3650) {
            this.termValue.add(Math.round((((float) currTerm/allTerm * tradeValue))));
            for (Map.Entry<String, int[]> entry: this.dict.entrySet()) {
                if (currTerm >= entry.getValue()[0] && currTerm <= entry.getValue()[1]) {
                    this.termYear.add(entry.getKey());
                }
            }
            return;
        } else {
            currTerm = currTerm - 3650;
            this.termYear.add("10Y");
            this.termValue.add(Math.round((((float) 3650/allTerm) * tradeValue)));
            this.calcTermValue(currTerm, allTerm, tradeValue);
        }
    }

    @Override
    public List<Transaction> process(final Transaction transaction) {
        List<Transaction> transactionList = new ArrayList<>();
        termYear = new ArrayList<>();
        termValue = new ArrayList<>();


        setDict();
        String tradeId = transaction.getTradeId();
        int currTerm = Integer.parseInt(transaction.getTerm());
        int allTerm = Integer.parseInt(transaction.getTerm());
        int tradeValue = Integer.parseInt(transaction.getTradeValue());
        String currency = transaction.getCurrency();
        calcTermValue(currTerm, allTerm, tradeValue);

        for (int i = 0; i < this.termYear.size(); i++) {
            Transaction t = new Transaction(tradeId, this.termYear.get(i), this.termValue.get(i).toString(), currency);
            transactionList.add(t);
        }

        return transactionList;
    }
}
