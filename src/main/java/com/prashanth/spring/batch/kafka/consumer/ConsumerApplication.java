package com.prashanth.spring.batch.kafka.consumer;

import com.prashanth.spring.batch.kafka.model.Customer;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.kafka.KafkaItemReader;
import org.springframework.batch.item.kafka.builder.KafkaItemReaderBuilder;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;

import java.util.List;
import java.util.Properties;

@Log4j2
@SpringBootApplication
@EnableBatchProcessing
@RequiredArgsConstructor
public class ConsumerApplication {
    public static void main(String[] args) {
        SpringApplication.run(ConsumerApplication.class, args);
    }

    private final StepBuilderFactory stepBuilderFactory;
    private final JobBuilderFactory jobBuilderFactory;
    private final KafkaProperties kafkaProperties;

    @Bean
    Job job() {
        return this.jobBuilderFactory.get("job")
                .start(start())
                .incrementer(new RunIdIncrementer())
                .build();
    }

    @Bean
    KafkaItemReader <Long, Customer> kafkaItemReader() {
        Properties properties = new Properties();
        properties.putAll(this.kafkaProperties.buildConsumerProperties());
        return new KafkaItemReaderBuilder<Long, Customer>()
                .partitions(1)
                .consumerProperties(properties)
                .name("customers-reader")
                .saveState(true)
                .topic("customers")
                .build();
    }

    @Bean
    Step start() {

        ItemWriter itemWriter = new ItemWriter<Customer>() {
            @Override
            public void write(List<? extends Customer> items) throws Exception {
                items.forEach(item -> log.info("new customer " + item));
            }
        };

        return stepBuilderFactory
                .get("step")
                .<Customer, Customer>chunk(10)
                .writer(itemWriter)
                .reader(kafkaItemReader())
                .build();
    }
}
