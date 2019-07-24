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
	private transient Logger log = LoggerFactory.getLogger(getClass());

	@NotNull
	@Valid
	private String csvHeader = new String();

	@Valid
	private boolean showHeader = true;

	/**
	 * Set the CSV header.
	 *
	 * @param csvHeader The CSV header row.
	 */
	public void setCsvHeader(String csvHeader)
	{
		this.csvHeader = Args.notNull(csvHeader.trim(), "CSV Header");
	}

	/**
	 * Get the CSV header.
	 *
	 * @return The CSV header row.
	 */
	public String getCsvHeader()
	{
		return csvHeader;
	}

	/**
	 * Set whether the CSV header should be in the output.
	 *
	 * @param showHeader True if CSV header should be included.
	 */
	public void setShowHeader(boolean showHeader)
	{
		this.showHeader = showHeader;
	}

	/**
	 * Get whether the CSV header is included in the output.
	 *
	 * @return True if CSV header is included.
	 */
	public boolean isShowHeader()
	{
		return showHeader;
	}

	/**
	 * Get whether the CSV header is included in the output.
	 * Convention says you should use {@code}is...()} for boolean
	 * values, so go use
	 * {@link JsonToFixedCSVService#isShowHeader} instead. This
	 * method is provided in case XStream isn't that smart.
	 *
	 * @return True if CSV header are included.
	 */
	public boolean getShowHeader()
	{
		return isShowHeader();
	}

	/**
	 * {@inheritDoc}.
	 */
	@Override
	public void doService(AdaptrisMessage msg) throws ServiceException
	{
		log.info("Starting JSON to CSV transformation");
		String jString = msg.getContent().trim();
		StringBuffer content = new StringBuffer();
		if (jString.startsWith("["))
		{
			JSONArray json = new JSONArray(new JSONTokener(jString));
			log.debug("JSON object is an array with length " + json.length());
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

		log.debug((showHeader ? "I" : "Not i") + "ncluding CSV headers");
		if (showHeader)
		{
			content.insert(0, csvHeader);
			content.insert(csvHeader.length(), '\n');
		}

		msg.setContent(content.toString(), msg.getContentEncoding());
		log.info("Finished JSON to CSV transformation");
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
		for (String header : csvHeader.split(","))
		{
			if (comma)
			{
				sb.append(',');
			}
			if (json.has(header))
			{
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
						value = "\"" + value + "\"";
					}
					sb.append(value);
				}
				else
				{
					sb.append(o.toString());
				}
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
