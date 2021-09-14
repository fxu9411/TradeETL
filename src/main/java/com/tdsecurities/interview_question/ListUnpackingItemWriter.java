package com.tdsecurities.interview_question;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStream;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.List;

public class ListUnpackingItemWriter<T> implements ItemWriter<List<T>>, ItemStream, InitializingBean {

    private ItemWriter<T> delegate;

    @Override
    public void write(final List<? extends List<T>> lists) throws Exception {
        final List<T> consolidatedList = new ArrayList<>();
        for (final List<T> list : lists) {
            consolidatedList.addAll(list);
        }
        open(new ExecutionContext());
        delegate.write(consolidatedList);
    }

    @Override
    public void afterPropertiesSet() {
        Assert.notNull(delegate, "You must set a delegate!");
    }

    @Override
    public void open(ExecutionContext executionContext) {
        if (delegate instanceof ItemStream) {
            ((ItemStream) delegate).open(executionContext);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) {
        if (delegate instanceof ItemStream) {
            ((ItemStream) delegate).update(executionContext);
        }
    }

    @Override
    public void close() {
        if (delegate instanceof ItemStream) {
            ((ItemStream) delegate).close();
        }
    }

    public void setDelegate(ItemWriter<T> delegate) {
        this.delegate = delegate;
    }

}