package com.tdsecurities.interview_question;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ExecutionContext;
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

import java.util.List;

/**
 * BatchConfigurator to build one job and two steps
 * Step 1: Read the input.csv and partition the trade by its term and trade value
 * Step 2: Read the partitioned file and Output to its currency files
 */
@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;


    /**
     * Reader 1 to read the original input file
     * @return FlatFileItemReader
     */
    @Bean
    @StepScope
    public FlatFileItemReader<Trade> reader() {
        FlatFileItemReader<Trade> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("input.csv"));
        reader.setLinesToSkip(1);
        reader.setLineMapper(new DefaultLineMapper() {
            {
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames(new String[] { "TradeId", "Term", "TradeValue", "Currency"});
                    }
                });
                setFieldSetMapper(new BeanWrapperFieldSetMapper<Trade>() {
                    {
                        setTargetType(Trade.class);
                    }
                });
            }
        });
        return reader;
    }

    /**
     * Reader 2 to read the partitioned file (i.e. splitted trades)
     * @return FlatFileItemReader
     */
    @Bean
    @StepScope
    public FlatFileItemReader<Trade> reader_2() {
        FlatFileItemReader<Trade> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource("input_partition.csv"));
        reader.setLinesToSkip(1);
        reader.setLineMapper(new DefaultLineMapper() {
            {
                setLineTokenizer(new DelimitedLineTokenizer() {
                    {
                        setNames(new String[] { "TradeId", "Term", "TradeValue", "Currency"});
                    }
                });
                setFieldSetMapper(new BeanWrapperFieldSetMapper<Trade>() {
                    {
                        setTargetType(Trade.class);
                    }
                });
            }
        });
        return reader;
    }

    /**
     * Call the TradeItemProcessor and split the trade
     * @return TradeItemProcessor
     */
    @Bean
    public TradeItemProcessor processor() {
        return new TradeItemProcessor();
    }

    /**
     * After split the trade, output to one csv file: input_partition.csv
     * @return FlatFileItemWriter
     */
    @Bean
    @StepScope
    public FlatFileItemWriter<Trade> flatFileItemWriter() {
        FlatFileItemWriter<Trade> writer = new FlatFileItemWriter<>();

        writer.setResource(new FileSystemResource("input_partition.csv"));

        writer.setHeaderCallback(writer1 -> writer1.write("TradeID,Term,TradeValue,Currency"));

        writer.setLineAggregator(new DelimitedLineAggregator<>(){{
            setDelimiter(",");
            setFieldExtractor(new BeanWrapperFieldExtractor<>() {{
                setNames(new String[]{"TradeId", "Term", "TradeValue", "Currency"});
            }});
        }});

        return writer;
    }

    /**
     * Build the custom ItemWriter to process list of Trades
     * @return ItemWriter
     */
    @Bean
    @StepScope
    public ItemWriter<List<Trade>> multiItemWriter() {
        var listUnpackingItemWriter = new ListUnpackingItemWriter<Trade>();
        listUnpackingItemWriter.setDelegate(flatFileItemWriter());
        return listUnpackingItemWriter;
    }

    /**
     * Build the collection of ItemWriters to output files to different destinations
     * @param itemWriterClassifier classifier to determine the output location
     * @return ClassifierCompositeItemWriter
     */
    @Bean
    @StepScope
    public ClassifierCompositeItemWriter<Trade> transactionItemWriter(
            Classifier<Trade, ItemWriter<? super Trade>> itemWriterClassifier
    ) {
        var compositeItemWriter = new ClassifierCompositeItemWriter<Trade>();
        compositeItemWriter.setClassifier(itemWriterClassifier);
        return compositeItemWriter;
    }


    /**
     * Build the classifier to set up the rule for file splitting
     * Based on the trade currency, output to its corresponding file
     * i.e. Trade with currency of CAD will be written to output_CAD.csv
     * @return Classifier
     */
    @Bean
    public Classifier<Trade, ItemWriter<? super Trade>> itemWriterClassifier() {
        return Trade -> {
            String fileName = "output_" + Trade.getCurrency() + ".csv";

            BeanWrapperFieldExtractor<Trade> fieldExtractor = new BeanWrapperFieldExtractor<>();
            fieldExtractor.setNames(new String[]{ "TradeId", "Term", "TradeValue", "Currency"});
            DelimitedLineAggregator<Trade> lineAggregator = new DelimitedLineAggregator<>();
            lineAggregator.setFieldExtractor(fieldExtractor);

            FlatFileItemWriter<Trade> itemWriter = new FlatFileItemWriter<>();
            itemWriter.setResource(new FileSystemResource(fileName));
            itemWriter.setAppendAllowed(true);
            itemWriter.setLineAggregator(lineAggregator);
            itemWriter.setHeaderCallback(writer -> writer.write("TradeID,Term,TradeValue,Currency"));

            itemWriter.open(new ExecutionContext());
            return itemWriter;
        };
    }


    /**
     * Build the job with two steps
     * @param step1 As the step 1 below
     * @param step2 As the step 2 below
     * @return Job
     */
    @Bean
    public Job readCSVFilesJob(Step step1, Step step2) {
        return jobBuilderFactory
                .get("readCSVFilesJob")
                .start(step1)
                .next(step2)
                .build();
    }

    /**
     * Step 1: Read the original file
     * convert the term from days to date range
     * and split the trades into multiple trade
     *
     * Output: input_partition.csv
     * @return Step
     */
    @Bean
    public Step step1() {
        return stepBuilderFactory.get("step1")
                .<Trade, List<Trade>>chunk(1)
                .reader(reader())
                .processor(processor())
                .writer(multiItemWriter())
                .build();
    }
    /**
     * Step 2: Read the partitioned file input_partition.csv
     * Based on the currency, stream to different ItemWriters
     * and ItemWriter determines which file to output
     *
     * Output: output_Currency.csv
     * @return Step
     */
    @Bean
    public Step step2() {
        return stepBuilderFactory.get("step2")
                .<Trade, Trade>chunk(1)
                .reader(reader_2())
                .writer(transactionItemWriter(itemWriterClassifier()))
                .build();
    }
}
