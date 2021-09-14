package com.tdsecurities.interview_question;

import java.util.*;


public class Transaction {
    private String TradeId;
    private String Term;
    private String TradeValue;
    private String Currency;

    public Transaction() {}

    public Transaction(String TradeId, String Term, String TradeValue, String Currency) {
        this.TradeId = TradeId;
        this.Term = Term;
        this.TradeValue = TradeValue;
        this.Currency = Currency;
    }

    public String getTradeId() {
        return this.TradeId;
    }

    public void setTradeId(String tradeId) {
        this.TradeId = tradeId;
    }

    public String getTerm() {
        return this.Term;
    }

    public void setTerm(String term) {
        this.Term = term;
    }

    public String getTradeValue() {
        return this.TradeValue;
    }

    public void setTradeValue(String tradeValue) {
        this.TradeValue = tradeValue;
    }

    public String getCurrency() {
        return this.Currency;
    }

    public void setCurrency(String currency) {
        this.Currency = currency;
    }
}