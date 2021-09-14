package com.tdsecurities.interview_question;

import java.io.Serializable;

/**
 * <h1>Trade</h1>
 * Implement the Trade object with standard getter and setter
 * <p>
 *
 *
 * @author  Weixuan(Frank) Xu
 * @version 1.0
 * @since   2021-09-14
 */

public class Trade implements Serializable {
    private String TradeId;
    private String Term;
    private String TradeValue;
    private String Currency;

    /**
     * Build the empty constructor for ItemReader to use.
     * Otherwise, an exception will be raised.
     */
    public Trade() {}

    public Trade(String TradeId, String Term, String TradeValue, String Currency) {
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