package com.example.demo;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.step.tasklet.MethodInvokingTaskletAdapter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
@EnableBatchProcessing
public class Batch {
	
	@Autowired
	public JobBuilderFactory jobBuilderFactory;
	
	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;
	
	/**
	 * csvファイルを読み込むメソッドです.
	 * 
	 * @return
	 */
	@Bean
	@StepScope
	public FlatFileItemReader<Fruit> reader(){
		FlatFileItemReader<Fruit> reader = new FlatFileItemReader<>();
		reader.setResource(new ClassPathResource("fruit_price.csv"));
		reader.setLineMapper(new DefaultLineMapper<Fruit>() {{
			setLineTokenizer(new DelimitedLineTokenizer() {{
				setNames(new String[] { "name" , "price" });
			}});
			setFieldSetMapper(new BeanWrapperFieldSetMapper<Fruit>() {{
				setTargetType(Fruit.class);
			}});
		}});
		
		return reader;
		
	}
	
	/**
	 * Stepの処理部分を返すメソッドです.
	 * 
	 * @return
	 */
	@Bean
	public FruitItemProcesser processer() {
		return new FruitItemProcesser();
	}
	
	// Writer
	@Bean
	public JdbcBatchItemWriter<Fruit> writer() {
		JdbcBatchItemWriter<Fruit> writer = new JdbcBatchItemWriter<>();
		writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
		writer.setSql("INSERT INTO fruit (name, price) VALUES (:name, :price)");
		writer.setDataSource(dataSource);
		return writer;
	}

	@Bean
	public JobExecutionListener listener() {
		return new JobStartEndLIstener(new JdbcTemplate(dataSource));
	}
	

	@Bean
	public Step truncateStep() {
		return stepBuilderFactory.get("truncateStep")
				.tasklet(truncateTasklet()).build();
	}
	
	@Bean
	public MethodInvokingTaskletAdapter truncateTasklet() {
		MethodInvokingTaskletAdapter adapter = new MethodInvokingTaskletAdapter();
		adapter.setTargetObject(truncateService());
		adapter.setTargetMethod("execute");
		return adapter;
	}
	
	@Bean
	public TruncateService truncateService() {
		return new TruncateServiceImpl();
	}
	
	// 書き込みステップ１
	@Bean
	public Step writerStep1() {
		return stepBuilderFactory.get("writerStep1")
				.<Fruit, Fruit>chunk(10)
				.reader(reader())
				.processor(processer())
				.writer(writer())
				.build();
	}
	
	@Bean
	public Step writerStep2() {
		return stepBuilderFactory.get("writerStep2")
				.<Fruit,Fruit> chunk(10)
				.reader(reader())
				.processor(processer())
				.writer(writer())
				.build();
	}
	
	// ジョブ
	@Bean
	public Job testJob() {
		return jobBuilderFactory.get("testJob")
				.incrementer(new RunIdIncrementer())
				.listener(listener())
				.flow(truncateStep())
				.next(writerStep1())
				.next(writerStep2())
				.end()
				.build();
	}
}
