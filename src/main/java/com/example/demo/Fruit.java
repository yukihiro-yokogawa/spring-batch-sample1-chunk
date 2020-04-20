package com.example.demo;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class Fruit {

	private String name;
	
	private int price;
	
	public Fruit(String name, int price) {
		this.name = name;
		this.price = price;
	}
	
}
