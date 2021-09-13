package com.tdsecurities.interview_question;

import org.springframework.batch.item.ItemProcessor;

import java.util.ArrayList;
import java.util.List;

public class TransactionItemProcessor implements ItemProcessor<Transaction, List<Transaction>> {

    @Override
    public List<Transaction> process(final Transaction transaction) {
        List<Transaction> transactionArrayList = new ArrayList<>();

        transaction.calcTermValue(Integer.parseInt(transaction.getTerm()));

        for (int i = 0; i < transaction.getTermYear().size(); i++) {
            Transaction t;
            t = new Transaction(transaction.getTradeId(), transaction.getTermYear().get(i), transaction.getTermValue().get(i).toString(), transaction.getCurrency());
            transactionArrayList.add(t);
        }

        return transactionArrayList;
    }
}
