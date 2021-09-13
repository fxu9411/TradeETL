package com.tdsecurities.interview_question;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.ClassifierCompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.classify.Classifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

import java.util.*;
import java.util.function.Function;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;


    @Bean
    public FlatFileItemReader<Transaction> reader() {
        FlatFileItemReader<Transaction> reader = new FlatFileItemReader<Transaction>();
        reader.setResource(new ClassPathResource("input.csv"));
        reader.setLinesToSkip(1);
        reader.setLineMapper(new DefaultLineMapper() {
            {
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames(new String[] { "TradeId", "Term", "TradeValue", "Currency"});
                    }
                });
                setFieldSetMapper(new BeanWrapperFieldSetMapper<Transaction>() {
                    {
                        setTargetType(Transaction.class);
                    }
                });
            }
        });
        return reader;
    }

    @Bean
    public ItemProcessor<Transaction, List<Transaction>> itemProcessor() {
        return transaction -> {
            List<Transaction> transactionArrayList = new ArrayList<>();

            transaction.calcTermValue(Integer.parseInt(transaction.getTerm()));

            for (int i = 0; i < transaction.getTermYear().size(); i++) {
                Transaction t;
                t = new Transaction(transaction.getTradeId(), transaction.getTermYear().get(i), transaction.getTermValue().get(i).toString(), transaction.getCurrency());
                transactionArrayList.add(t);
            }

            return transactionArrayList;
        };
    }

    @Bean
    public FlatFileItemWriter<List<Transaction>> flatFileItemWriter() {
        return trans -> {
            for (List<Transaction> transaction: trans) {
                for (Transaction t : transaction) {
                    FlatFileItemWriter t = new FlatFileItemWriter();
                }
            }
        };
    }

    @Bean
    public ClassifierCompositeItemWriter<Transaction> TransactionItemWriter(Classifier<Transaction, ItemWriter<? super Transaction>> itemWriterClassifier) {
        ClassifierCompositeItemWriter<Transaction> compositeItemWriter = new ClassifierCompositeItemWriter<>();
        compositeItemWriter.setClassifier(itemWriterClassifier());
        return compositeItemWriter;
    }


    @Bean
    public Classifier<Transaction, ItemWriter<? super Transaction>> itemWriterClassifier() {
        Map<String, FlatFileItemWriter> dict = new HashMap<String, FlatFileItemWriter>();
        return TransactionList -> {
            for (Transaction transaction: TransactionList) {
                String key = transaction.getCurrency().toUpperCase();
                String fileName = "output_" + key + ".csv";

                BeanWrapperFieldExtractor<Transaction> fieldExtractor = new BeanWrapperFieldExtractor<>();
                fieldExtractor.setNames(new String[]{"TradeId", "Term", "TradeValue", "Currency"});
                DelimitedLineAggregator<Transaction> lineAggregator = new DelimitedLineAggregator<>();
                lineAggregator.setFieldExtractor(fieldExtractor);

                FlatFileItemWriter<Transaction> itemWriter = dict.get(key);

                if (itemWriter != null) {
                    itemWriter.setHeaderCallback(null);
                } else {
                    itemWriter = new FlatFileItemWriter<>();
                    itemWriter.setResource(new FileSystemResource(fileName));
                    itemWriter.setAppendAllowed(true);
                    itemWriter.setLineAggregator(lineAggregator);
                    itemWriter.setHeaderCallback(writer -> writer.write("TradeID,Term,TradeValue,Currency"));
                    dict.put(key, itemWriter);
                }

                itemWriter.open(new ExecutionContext());
                return itemWriter;
            }
            return null;
        }
    }

    @Bean
    public TransactionItemProcessor processor() {
        return new TransactionItemProcessor();
    }

    @Bean
    public ConsoleItemWriter<Transaction> writer() {
        return new ConsoleItemWriter<Transaction>();
    }


    @Bean
    public Job readCSVFilesJob() {
        return jobBuilderFactory
                .get("readCSVFilesJob")
                .start(step1())
                .build();
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1").<Transaction, Transaction>chunk(5)
                .reader(reader())
                .processor((Function<? super Transaction, ? extends Transaction>) processor())
                .writer(writer())
                .build();
    }
}
