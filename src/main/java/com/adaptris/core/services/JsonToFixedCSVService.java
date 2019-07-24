package com.adaptris.core.services;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.ServiceImp;
import com.adaptris.core.util.Args;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
/**
 * Marshall the JSON doc to CSV but maintain the field ordering. We do
 * currently have JSON to CSV services (interlok-csv-json), but we need
 * a custom service for this.
 * <p>
 * Something along the lines of the following should do:
 * <p>
 * <code>JsonToFixedCSVService
 *   List<String> csvHeaders
 *   doService()
 *     if array:
 *       for each element:
 *         marshalToCSV(element)
 *     elseif map:
 *       marshalToCSV(element)
 *     else:output nothing
 *     log msg
 *   marshalToCSV(element)
 *     for each header in csvHeaders:
 *       element.get(header) // output blank if not found</code>
 */
@XStreamAlias("json-to-fixed-csv-service")
@AdapterComponent
@ComponentProfile(summary = "Transform a JSON document to CSV", tag = "service,transform,json,csv")
public class JsonToFixedCSVService extends ServiceImp
{
	private transient Logger log = LoggerFactory.getLogger(this.getClass());

	@NotNull
	@Valid
	private List<String> csvHeaders = new ArrayList<>();

	@Valid
	private boolean showHeaders = true;

	/**
	 * Set the CSV headers.
	 *
	 * @param csvHeaders A list of the CSV headers.
	 */
	public void setCsvHeaders(List<String> csvHeaders)
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
	 * Set whether the CSV headers should be in the output.
	 *
	 * @param showHeaders True if CSV headers should be included.
	 */
	public void setShowHeaders(boolean showHeaders)
	{
		this.showHeaders = showHeaders;
	}

	/**
	 * Get whether the CSV headers are included in the output.
	 *
	 * @return True if CSV headers are included.
	 */
	public boolean isShowHeaders()
	{
		return showHeaders;
	}

	/**
	 * Get whether the CSV headers are included in the output.
	 * Convention says you should use {@code}is...()} for boolean
	 * values, so go use
	 * {@link JsonToFixedCSVService#isShowHeaders} instead. This
	 * method is provided in case XStream isn't that smart.
	 *
	 * @return True if CSV headers are included.
	 */
	public boolean getShowHeaders()
	{
		return isShowHeaders();
	}

	/**
	 * {@inheritDoc}.
	 */
	@Override
	public void doService(AdaptrisMessage msg) throws ServiceException
	{
		String jString = msg.getContent().trim();
		StringBuffer content = new StringBuffer();
		if (jString.startsWith("["))
		{
			JSONArray json = new JSONArray(new JSONTokener(jString));
			for (int i = 0; i < json.length(); i++)
			{
				content.append(marshalToCSV(json.getJSONObject(i)));
			}
		}
		else if (jString.startsWith("{"))
		{
			JSONObject json = new JSONObject(new JSONTokener(jString));
			content.append(marshalToCSV(json));
		}

		StringBuffer headers = new StringBuffer();
		if (showHeaders)
		{
			for (String header : csvHeaders)
			{
				if (headers.length() > 0)
				{
					headers.append(',');
				}
				headers.append(header);
			}
			headers.append('\n');
		}

		msg.setContent(headers.append(content).toString(), msg.getContentEncoding());
	}

	/**
	 * Marshal a JSON object to CSV
	 *
	 * @param json The JSON to convert to CSV.
	 * @return The CSV row.
	 */
	private String marshalToCSV(JSONObject json)
	{
		StringBuffer sb = new StringBuffer();
		boolean comma = false;
		for (String header : csvHeaders)
		{
			if (comma)
			{
				sb.append(',');
			}
			Object o = json.get(header);
			if (o instanceof String)
			{
				// See RFC 4180 for further details.
				String value = (String)o;
				/*
				 * if double-quotes are used to enclose fields, then a
				 * double-quote appearing inside a field must be
				 * escaped by preceding it with another double quote:
				 * "aaa","b""bb","ccc"
				 */
				if (value.contains("\""))
				{
					value = "\"" + value.replaceAll("\"", "\"\"") + "\"";
				}
				/*
				 * fields containing commas should be enclosed in
				 * double-quotes:
				 * foo,"bar,baz"
				 */
				if (value.contains(","))
				{
					value = "\"" + value + "\"";
				}
				/*
				 * fields containing line breaks, double quotes should
				 * be enclosed in double-quotes:
				 * foo,"bar\nbaz"
				 */
				if (value.contains("\n") || value.contains("\r"))
				{
					//value = "\"" + value.replaceAll("\n", "\\\\n").replaceAll("\r", "\\\\r") + "\"";
					value = "\"" + value + "\"";
				}
				sb.append(value);
			}
			else
			{
				sb.append(o.toString());
			}
			comma = true;
		}
		return sb.append('\n').toString();
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
