package com.example.demo;

import org.springframework.batch.core.ExitStatus;

public interface TruncateService {

	ExitStatus execute();
	
}
