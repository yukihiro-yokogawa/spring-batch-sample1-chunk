package com.example.demo;

import org.springframework.batch.item.ItemProcessor;

/**
 * @author 黄金金魚
 * ItemReaderで読み込んだデータを加工するクラスです.
 */
public class FruitItemProcessor implements ItemProcessor<Fruit, Fruit> {

	/**
	 * ItemReaderで読み込んだデータを加工するメソッドです.
	 * 読み込んだ文字列を大文字に加工します.
	 */
	@Override
	public Fruit process(final Fruit fruit) throws Exception{
		final String title = fruit.getName().toUpperCase();
		final int price = fruit.getPrice();
		final Fruit transformColumns = new Fruit(title,price);
		return transformColumns;
	}
		
}
