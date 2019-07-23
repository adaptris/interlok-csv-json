package com.adaptris.core.services;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.core.util.Args;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.ServiceImp;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.List;

@XStreamAlias("json-to-fixed-csv-service")
@AdapterComponent
@ComponentProfile(summary = "Transform a JSON document to CSV", tag = "service,transform,json,csv")
public class JsonToFixedCSVService extends ServiceImp
{
	private transient Logger log = LoggerFactory.getLogger(this.getClass());

	@NotNull
	@Valid
	private List<String> csvHeaders;

	/**
	 * Set the CSV headers.
	 *
	 * @param csvHeaders A list of the CSV headers.
	 */
	public void setCsvHeader(List<String> csvHeaders)
	{
		this.csvHeaders = Args.notNull(csvHeaders, "CSV Headers");
	}

	/**
	 * Get the CSV headers.
	 *
	 * @return A list of CSV headers.
	 */
	public List<String> getCsvHeaders()
	{
		return csvHeaders;
	}

	/**
	 * {@inheritDoc}.
	 */
	@Override
	public void doService(AdaptrisMessage msg) throws ServiceException
	{
		String jString = msg.getContent().trim();
		if (jString.startsWith("["))
		{
			JSONArray json = new JSONArray(new JSONTokener(jString));
			for (int i = 0; i < json.length(); i++)
			{
				marshalToCSV(json.getJSONObject(i));
			}
		}
		else if (jString.startsWith("{"))
		{
			marshalToCSV(new JSONObject(new JSONTokener(jString)));
		}

		/*
		 * if array:
		 *     for each:
		 *         marshalToCSV()
		 * else if map:
		 *     marshalToCSV()
		 * else:
		 *     output nothing
		 * log message
		 *
		 */
	}

	private void marshalToCSV(JSONObject json)
	{
		for (String header : csvHeaders)
		{
			String jValue = (String)json.get(header);
		}
		/*
		 * for each header:
		 *     element.get(header) (blank if not found)
		 */
	}

	/**
	 * {@inheritDoc}.
	 */
	@Override
	protected void initService() throws CoreException
	{
		/* not implemented */
	}

	/**
	 * {@inheritDoc}.
	 */
	@Override
	protected void closeService()
	{
		/* not implemented */
	}

	/**
	 * {@inheritDoc}.
	 */
	@Override
	public void prepare() throws CoreException
	{
		/* not implemented */
	}
}
