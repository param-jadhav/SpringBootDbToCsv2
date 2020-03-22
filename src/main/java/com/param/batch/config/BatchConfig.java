package com.param.batch.config;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;

import com.param.batch.model.Person;
import com.param.batch.processor.PersonItemProcessor;

@Configuration
@EnableBatchProcessing
public class BatchConfig<BasicDataSource> {

	//ref doc:- https://dzone.com/articles/spring-batch-flatfileitemwriter-hacking
	@Autowired
	private JobBuilderFactory jobBuilderFactory;

	@Autowired
	private StepBuilderFactory stepBuilderFactory;

	@Autowired
	private DataSource dataSource;

	@Bean
	public JdbcCursorItemReader<Person> reader() {
		JdbcCursorItemReader<Person> cursorItemReader = new JdbcCursorItemReader<>();
		cursorItemReader.setDataSource(dataSource);
		cursorItemReader.setSql(
				"SELECT person_id,first_name,last_name,email,age FROM person");
		cursorItemReader.setRowMapper(new PersonRowMapper());
		return cursorItemReader;
	}

	@Bean
	public PersonItemProcessor processor() {
		return new PersonItemProcessor();
	}
	public static StringBuilder fileGenerator() {
		StringBuilder fileName = new StringBuilder("person_");
		DateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
		Date d = new Date();
		fileName.append(dateFormat.format(d));
		fileName.append(".csv");
		System.out.println(fileName);
		return fileName;
	}
	@Bean
	public FlatFileItemWriter<Person> writer() {
		FlatFileItemWriter<Person> writer = new FlatFileItemWriter<Person>();
		// writer.setResource(new ClassPathResource("person2.csv"));//
		// C:\Users\prameshwar\Desktop\batch_file_csv
		StringBuilder fName = BatchConfig.fileGenerator();
		writer.setResource(new FileSystemResource(
				"C:\\Users\\prameshwar\\Desktop\\batch_file_csv\\" + fName));
		// FlatFileHeaderCallback   // ref:- https://www.javatips.net/api/org.springframework.batch.item.file.flatfileheadercallback
		// FlatFileFooterCallback
		DelimitedLineAggregator<Person> lineAggregator = new DelimitedLineAggregator<Person>();
		lineAggregator.setDelimiter("|");

		BeanWrapperFieldExtractor<Person> fieldExtractor = new BeanWrapperFieldExtractor<Person>();

		fieldExtractor.setNames(new String[]{"personId", "firstName",
				"lastName", "email", "age"});
		lineAggregator.setFieldExtractor(fieldExtractor);

		writer.setLineAggregator(lineAggregator);
		return writer;
	}

	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1").<Person, Person>chunk(5)
				.reader(reader()).processor(processor()).writer(writer())
				.build();
	}

	@Bean
	public Job exportPerosnJob() {
		return jobBuilderFactory.get("exportPeronJob")
				.incrementer(new RunIdIncrementer()).flow(step1()).end()
				.build();
	}
}

class PersonRowMapper implements RowMapper<Person> {

	@Override
	public Person mapRow(ResultSet rs, int rowNum) throws SQLException {
		Person person = new Person();
		person.setPersonId(rs.getInt("person_id"));
		person.setFirstName(rs.getString("first_name"));
		person.setLastName(rs.getString("last_name"));
		person.setEmail(rs.getString("email"));
		person.setAge(rs.getInt("age"));
		return person;
	}

}
