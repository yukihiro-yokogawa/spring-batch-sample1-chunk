package com.example.demo;

import org.springframework.batch.core.ExitStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

public class TruncateServiceImpl implements TruncateService{

	@Autowired
	JdbcTemplate jdbcTemplate;
	
	public ExitStatus execute() {
		jdbcTemplate.execute("TRUNCATE TABLE fruit");
		return ExitStatus.COMPLETED;
	}
	
}
