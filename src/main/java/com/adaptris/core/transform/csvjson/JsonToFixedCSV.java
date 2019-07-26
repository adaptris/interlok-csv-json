package com.adaptris.core.transform.csvjson;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.ServiceImp;
import com.adaptris.core.util.Args;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Marshall the JSON doc to CSV but maintain the field ordering. We do
 * currently have JSON to CSV services (interlok-csv-json), but we need
 * a custom service for this.
 * <p>
 * Something along the lines of the following should do:
 * <p>
 * <code>JsonToFixedCSV
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
@ComponentProfile(summary = "Transform a JSON document to CSV with a fixed header", tag = "service,transform,json,csv")
public class JsonToFixedCSV extends ServiceImp
{
	private transient Logger log = LoggerFactory.getLogger(getClass());

	@NotNull
	@Valid
	private String csvHeader = new String();

	private transient List<String> header;

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
		header = new ArrayList<>();
		for (String h : this.csvHeader.split(","))
		{
			header.add(h);
		}
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
	 * {@link JsonToFixedCSV#isShowHeader} instead. This
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
		try(CSVPrinter csv = new CSVPrinter(new StringBuffer(), CSVFormat.DEFAULT))
		{
			log.debug((showHeader ? "I" : "Not i") + "ncluding CSV headers");
			if (showHeader)
			{
				csv.printRecord(header);
			}

			String jString = msg.getContent();
			if (jString.startsWith("["))
			{
				JSONArray json = new JSONArray(new JSONTokener(jString));
				log.debug("JSON object is an array with length " + json.length());
				for (int i = 0; i < json.length(); i++)
				{
					csv.printRecord(marshalToCSV(json.getJSONObject(i)));
				}
			}
			else if (jString.startsWith("{"))
			{
				JSONObject json = new JSONObject(new JSONTokener(jString));
				csv.printRecord(marshalToCSV(json));
			}

			msg.setContent(csv.getOut().toString(), msg.getContentEncoding());
		}
		catch (IOException e)
		{
			log.error("Problem generating CSV data", e);
		}
		finally
		{
			log.info("Finished JSON to CSV transformation");
		}
	}

	/**
	 * Marshal a JSON object to CSV.
	 *
	 * @param json The JSON to convert to CSV.
	 * @return The CSV row as a list of Strings.
	 */
	private List<String> marshalToCSV(JSONObject json)
	{
		List<String> record = new ArrayList<>();
		for (String header : header)
		{
			if (json.has(header))
			{
				record.add(json.get(header).toString());
			}
			else
			{
				record.add("");
			}
		}
		return record;
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
