package com.prashanth.spring.batch.kafka.producer;

import com.prashanth.spring.batch.kafka.model.Customer;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.kafka.KafkaItemReader;
import org.springframework.batch.item.kafka.KafkaItemWriter;
import org.springframework.batch.item.kafka.builder.KafkaItemWriterBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.convert.converter.Converter;
import org.springframework.kafka.core.KafkaTemplate;

@EnableBatchProcessing
@SpringBootApplication
@RequiredArgsConstructor
public class ProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final KafkaTemplate<Long, Customer> kafkaTemplate;

    @Bean
    Job job() {
        return this.jobBuilderFactory.get("job")
                .start(start())
                .incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean
    KafkaItemWriter<Long, Customer> kafkaItemWriter() {
        return new KafkaItemWriterBuilder<Long, Customer>()
                .kafkaTemplate(kafkaTemplate)
                .itemKeyMapper(new Converter<Customer, Long>() {
                    @Override
                    public Long convert(Customer source) {
                        return source.getId();
                    }
                })
                .build();
    }

    @Bean
    Step start() {

        ItemReader itemReader = new ItemReader<Customer>() {
            @Override
            public Customer read() throws Exception {
                return null;
            }
        };

        return this.stepBuilderFactory.get("S1")
                .<Customer, Customer>chunk(10)
                .reader(itemReader)
                .writer(kafkaItemWriter())
                .build();
    }
}
