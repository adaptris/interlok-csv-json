package com.adaptris.core.services;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import org.junit.Test;

public class JsonToFixedCSVServiceTest
{
	private static final String JSON_ARRAY = "";
	private static final String CSV_ARRAY = "";

	private static final String JSON_OBJECT = "";
	private static final String CSV_OBJECT = "";

	@Test
	public void testServiceWithArray()
	{
		AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(JSON_ARRAY);
	}

	@Test
	public void testServiceWithObject()
	{
		AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(JSON_OBJECT);
	}
}
