package com.tdsecurities.interview_question;

import java.util.*;

public class Transaction {
    private String TradeId;
    private String Term;
    private String TradeValue;
    private String Currency;

    private HashMap<String, int[]> dict;
    private ArrayList<String> termYear;
    private ArrayList<Integer> termValue;

    public Transaction() {

    }

    public Transaction(String TradeId, String Term, String TradeValue, String Currency) {
        this.TradeId = TradeId;
        this.Term = Term;
        this.TradeValue = TradeValue;
        this.Currency = Currency;
        this.dict = new HashMap<>();
        this.dict.put("3M", new int[]{0, 90});
        this.dict.put("6M", new int[]{91, 180});
        this.dict.put("1Y", new int[]{181, 365});
        this.dict.put("2Y", new int[]{366, 730});
        this.dict.put("5Y", new int[]{731, 1825});
        this.dict.put("10Y", new int[]{1826, 3650});
        this.termYear = new ArrayList<>();
        this.termValue = new ArrayList<>();
    }

    public String getTradeId() {
        return TradeId;
    }

    public void setTradeId(String tradeId) {
        this.TradeId = tradeId;
    }

    public String getTerm() {
        return Term;
    }

    public void setTerm(String term) {
        this.Term = term;
    }

    public ArrayList<String> getTermYear() {
        return termYear;
    }

    public void setTermYear(ArrayList<String> termYear) {
        this.termYear = termYear;
    }

    public ArrayList<Integer> getTermValue() {
        return termValue;
    }

    public void setTermValue(ArrayList<Integer> termValue) {
        this.termValue = termValue;
    }

    public String getTradeValue() {
        return TradeValue;
    }

    public void setTradeValue(String tradeValue) {
        this.TradeValue = tradeValue;
    }

    public String getCurrency() {
        return Currency;
    }

    public void setCurrency(String Currency) {
        this.Currency = Currency;
    }

    public void calcTermValue(int term) {
        if (term < 0) {
            return;
        } else if (term <= 3650) {
            this.termValue.add(Math.round((((float) term/Integer.parseInt(this.Term)) * Integer.parseInt(this.TradeValue))));

            for (Map.Entry<String, int[]> entry: this.dict.entrySet()) {
                if (term >= entry.getValue()[0] && term <= entry.getValue()[1]) {
                    this.termYear.add(entry.getKey());
                }
            }
            return;
        } else {
            term = term - 3650;
            this.termYear.add("10Y");
            this.termValue.add(Math.round((((float) 3650/Integer.parseInt(this.Term)) * Integer.parseInt(this.TradeValue))));
            this.calcTermValue(term);
        }
    }

    @Override
    public String toString() {
        return this.getTradeId() + "\t" + this.getTerm() + "\t" + this.getTradeValue() + "\t" + this.getCurrency() + "\t";
    }
}