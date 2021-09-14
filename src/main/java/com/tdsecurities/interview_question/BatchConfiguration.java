package com.tdsecurities.interview_question;

import com.tdsecurities.interview_question.Transaction;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
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

import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;
import java.util.List;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
    @Autowired
    public JobBuilderFactory jobBuilderFactory;

    @Autowired
    public StepBuilderFactory stepBuilderFactory;


    @Bean
    @StepScope
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
    @StepScope
    public FlatFileItemReader<Transaction> reader_2() {
        FlatFileItemReader<Transaction> reader = new FlatFileItemReader<Transaction>();
        reader.setResource(new ClassPathResource("input_partition.csv"));
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
    public ClassifierCompositeItemWriter<Transaction> TransactionItemWriter(
            Classifier<Transaction, ItemWriter<? super Transaction>> itemWriterClassifier
    ) {
        ClassifierCompositeItemWriter<Transaction> compositeItemWriter = new ClassifierCompositeItemWriter<>();
        compositeItemWriter.setClassifier(itemWriterClassifier);
        return compositeItemWriter;
    }

    @Bean
    public Classifier<Transaction, ItemWriter<? super Transaction>> itemWriterClassifier() {
        return Transaction -> {
            String fileName = Transaction.getCurrency();

            BeanWrapperFieldExtractor<Transaction> fieldExtractor = new BeanWrapperFieldExtractor<>();
            fieldExtractor.setNames(new String[]{"recId", "name"});
            DelimitedLineAggregator<Transaction> lineAggregator = new DelimitedLineAggregator<>();
            lineAggregator.setFieldExtractor(fieldExtractor);

            FlatFileItemWriter<Transaction> itemWriter = new FlatFileItemWriter<>();
            itemWriter.setResource(new FileSystemResource(fileName));
            itemWriter.setAppendAllowed(true);
            itemWriter.setLineAggregator(lineAggregator);
            itemWriter.setHeaderCallback(writer -> writer.write("REC_ID,NAME"));

            itemWriter.open(new ExecutionContext());
            return itemWriter;
        };
    }

    @Bean
    public TransactionItemProcessor processor() {
        return new TransactionItemProcessor();
    }

    @Bean
    @StepScope
    public FlatFileItemWriter<Transaction> flatFileItemWriter() {
        FlatFileItemWriter<Transaction> writer = new FlatFileItemWriter<Transaction>();

        writer.setResource(new FileSystemResource("input_partition.csv"));

        writer.setHeaderCallback(new FlatFileHeaderCallback() {
            @Override
            public void writeHeader(Writer writer) throws IOException {
                writer.write("TradeID,Term,TradeValue,Currency");
            }
        });

        writer.setLineAggregator(new DelimitedLineAggregator<>(){{
            setDelimiter(",");
            setFieldExtractor(new BeanWrapperFieldExtractor<Transaction>(){{
                setNames(new String[] { "TradeId", "Term", "TradeValue", "Currency"});
            }});
        }});

        return writer;
    }

    @Bean
    @StepScope
    public ItemWriter<List<Transaction>> multiItemWriter() {
        ListUnpackingItemWriter<Transaction> listUnpackingItemWriter = new ListUnpackingItemWriter<Transaction>();
        listUnpackingItemWriter.setDelegate(flatFileItemWriter());
        return listUnpackingItemWriter;
    }

    @Bean
    public Job readCSVFilesJob(Step step1, Step step2) {
        return jobBuilderFactory
                .get("readCSVFilesJob")
                .start(step1)
                .next(step2)
                .build();
    }

    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<Transaction, List<Transaction>>chunk(1)
                .reader(reader())
                .processor(processor())
                .writer(multiItemWriter())
                .build();
    }

    @Bean
    public Step step2() {
        return stepBuilderFactory.get("step2")
                .chunk(1)
                .reader(reader_2())
                .writer(TransactionItemWriter(itemWriterClassifier(null)))
                .build();
    }
}
