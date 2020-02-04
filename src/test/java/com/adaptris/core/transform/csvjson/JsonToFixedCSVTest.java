package com.adaptris.core.transform.csvjson;

import static org.junit.Assert.fail;
import java.io.IOException;
import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Test;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceCase;
import com.adaptris.core.services.splitter.json.JsonArraySplitter;
import com.adaptris.core.services.splitter.json.LargeJsonArraySplitter;

public class JsonToFixedCSVTest extends ServiceCase
{
	private static final String CSV_HEADER = "sentence_1,sentence_2,sentence_3,sentence_4,sentence_5,sentence_6,sentence_7";

	private static final String JSON_ARRAY = "array.json";
	private static final String CSV_ARRAY_HEADER = "array-header.csv";
	private static final String CSV_ARRAY = "array.csv";

	private static final String JSON_OBJECT = "object.json";
	private static final String CSV_OBJECT_HEADER = "object-header.csv";
	private static final String CSV_OBJECT = "object.csv";

    @Override
    public boolean isAnnotatedForJunit4() {
      return true;
    }
    
	/**
	 * Test that a JSON array becomes several lines on CSV,
	 * and displaying CSV header column names.
	 *
	 * @throws Exception
	 */
	@Test
	public void testArrayWithHeader() throws Exception
	{
		AdaptrisMessage message = getMessage(JSON_ARRAY);
		JsonToFixedCSV service = getService(true, CSV_HEADER);
		service.setMessageSplitter(new LargeJsonArraySplitter());

		execute(service, message);

		Assert.assertEquals(LargeJsonArraySplitter.class, service.getMessageSplitter().getClass());
		Assert.assertEquals(getResource(CSV_ARRAY_HEADER), message.getContent());
	}

	/**
	 * Test that a JSON array becomes several lines on CSV,
	 * and not displaying CSV header column names.
	 *
	 * @throws Exception
	 */
	@Test
	public void testArrayNoHeader() throws Exception
	{
		AdaptrisMessage message = getMessage(JSON_ARRAY);
		JsonToFixedCSV service = getService(false, CSV_HEADER);
		service.setMessageSplitter(new JsonArraySplitter());

		execute(service, message);

		Assert.assertEquals(getResource(CSV_ARRAY), message.getContent());
	}

	/**
	 * Test that a JSON object becomes CSV data,
	 * and displaying CSV header column names.
	 *
	 * @throws Exception
	 */
	@Test
	public void testObjectWithHeader() throws Exception
	{
		AdaptrisMessage message = getMessage(JSON_OBJECT);
		JsonToFixedCSV service = getService(true, CSV_HEADER);

		execute(service, message);

		Assert.assertEquals(getResource(CSV_OBJECT_HEADER), message.getContent());
	}

	/**
	 * Test that a JSON object becomes CSV data,
	 * and not displaying CSV header column names.
	 *
	 * @throws Exception
	 */
	@Test
	public void testObjectNoHeader() throws Exception
	{
		AdaptrisMessage message = getMessage(JSON_OBJECT);
		JsonToFixedCSV service = getService(false, CSV_HEADER);

		execute(service, message);

		Assert.assertEquals(getResource(CSV_OBJECT), message.getContent());
	}

	/**
	 * Test that the service behaves as expected if bad JSON is given
	 * to it.
	 *
	 * @throws Exception
	 */
	@Test
	public void testNotJson() throws Exception
	{
		try
		{
			AdaptrisMessage message = AdaptrisMessageFactory.getDefaultInstance().newMessage("This isn't JSON");
			JsonToFixedCSV service = getService(false, CSV_HEADER);

			execute(service, message);
			fail();
		}
		catch (CoreException e)
		{
			/* expected */
		}
	}

	private AdaptrisMessage getMessage(String resource) throws IOException
	{
		return AdaptrisMessageFactory.getDefaultInstance().newMessage(getResource(resource));
	}

	private String getResource(String resource) throws IOException
	{
		return IOUtils.toString(getClass().getResourceAsStream(resource), "UTF-8");
	}

	private JsonToFixedCSV getService(boolean showHeader, String header)
	{
		JsonToFixedCSV service = (JsonToFixedCSV)retrieveObjectForSampleConfig();
		service.setShowHeader(showHeader);
		service.setCsvHeader(header);
		return service;
	}

	@Override
	protected Object retrieveObjectForSampleConfig()
	{
		return new JsonToFixedCSV("field1,field2");
	}
}
