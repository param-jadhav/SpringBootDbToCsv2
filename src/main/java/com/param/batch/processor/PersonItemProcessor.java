package com.param.batch.processor;

import org.springframework.batch.item.ItemProcessor;

import com.param.batch.model.Person;

public class PersonItemProcessor implements ItemProcessor<Person, Person> {

	@Override
	public Person process(Person person) throws Exception {
		return person;
	}
}
