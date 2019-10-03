package com.adaptris.core.transform.csvjson;

import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.ServiceImp;
import com.adaptris.core.json.JsonUtil;
import com.adaptris.core.services.splitter.LineCountSplitter;
import com.adaptris.core.services.splitter.MessageSplitter;
import com.adaptris.core.services.splitter.MessageSplitterImp;
import com.adaptris.core.services.splitter.json.JsonArraySplitter;
import com.adaptris.core.services.splitter.json.JsonObjectSplitter;
import com.adaptris.core.services.splitter.json.LargeJsonArraySplitter;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.CloseableIterable;
import com.adaptris.interlok.InterlokException;
import com.adaptris.interlok.config.DataInputParameter;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.BooleanUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
@ComponentProfile(summary = "Transform a JSON document to CSV with a fixed header",
		tag = "service,transform,json,csv",
		recommended = { JsonArraySplitter.class, JsonObjectSplitter.class, LargeJsonArraySplitter.class, LineCountSplitter.class })
public class JsonToFixedCSV extends ServiceImp
{
	private transient Logger log = LoggerFactory.getLogger(getClass());

	@NotNull
	@Valid
	private DataInputParameter<String>  csvHeader;

	@Valid
	@InputFieldDefault(value = "true")
	private Boolean showHeader;

	@NotNull
	@Valid
	private MessageSplitterImp messageSplitter = new JsonObjectSplitter();

	/**
	 * Set the CSV header.
	 *
	 * @param csvHeader The CSV header row.
	 */
	public void setCsvHeader(DataInputParameter<String> csvHeader)
	{
		this.csvHeader = Args.notNull(csvHeader, "CSV Header");
	}

	/**
	 * Get the CSV header.
	 *
	 * @return The CSV header row.
	 */
	public DataInputParameter<String> getCsvHeader()
	{
		return csvHeader;
	}

	/**
	 * Set whether the CSV header should be in the output.
	 *
	 * @param showHeader True if CSV header should be included.
	 */
	public void setShowHeader(Boolean showHeader)
	{
		this.showHeader = showHeader;
	}

	/**
	 * Get whether the CSV header is included in the output.
	 *
	 * @return True if CSV header is included.
	 */
	public Boolean isShowHeader()
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
	public Boolean getShowHeader()
	{
		return isShowHeader();
	}

	protected boolean showHeaders() {
	  return BooleanUtils.toBooleanDefaultIfNull(getShowHeader(), true);
	}

	/**
	 * Set the splitter to use to divvy up the original message if it's
	 * particularly large.
	 *
	 * @param messageSplitter The message splitter.
	 */
	public void setMessageSplitter(MessageSplitterImp messageSplitter)
	{
		this.messageSplitter = Args.notNull(messageSplitter, "Message Splitter");
	}

	/**
	 * Get the splitter used to divvy up the original message if it's
	 * particularly large.
	 *
	 * @return The message splitter.
	 */
	public MessageSplitter getMessageSplitter()
	{
		return messageSplitter;
	}

	/**
	 * {@inheritDoc}.
	 */
	@Override
	public void doService(AdaptrisMessage message) throws ServiceException
	{
		log.info("Starting JSON to CSV transformation with" + (showHeaders() ? "" : "out") + " header");

		try(CSVPrinter csv = new CSVPrinter(message.getWriter(), CSVFormat.DEFAULT))
		{
			if (showHeaders())
			{
				csv.printRecord(header(message));
			}

			messageSplitter.setMessageFactory(AdaptrisMessageFactory.getDefaultInstance());
			try (CloseableIterable<AdaptrisMessage> splitMessages = CloseableIterable.ensureCloseable(messageSplitter.splitMessage(message)))
			{
				if (messageSplitter.getClass().equals(LargeJsonArraySplitter.class) || messageSplitter.getClass().equals(JsonArraySplitter.class))
				{
					log.debug("Split JSON array into series of messages");
					for (AdaptrisMessage splitMessage : splitMessages)
					{
						Map<String, String> jMap = JsonUtil.mapifyJson(splitMessage);
						log.debug("JSON object has " + jMap.size() + " keys");
						csv.printRecord(marshalToCSV(message, jMap));
					}
				}
				else
				{
					// handles a JSON object but not a JSON array
					log.debug("Split JSON message into series of messages ");
					Map<String, String> jMap = new HashMap<>();
					for (AdaptrisMessage splitMessage : splitMessages)
					{
						Map<String, String> map = JsonUtil.mapifyJson(splitMessage);
						log.debug("JSON object has another " + map.size() + " keys");
						jMap.putAll(map);
					}
					csv.printRecord(marshalToCSV(message, jMap));
				}
			}
		}
		catch (Exception e)
		{
			log.error("Could not parse JSON nor generate CSV data", e);
			throw new ServiceException(e);
		}
		finally
		{
			log.info("Finished JSON to CSV transformation");
		}
	}

	/**
	 * Marshal part of a large JSON array to CSV.
	 *
	 *
	 * @param message
	 * @param jMap The map of this particular JSON object.
	 *
	 * @return The CSV row as a list of Strings.
	 */
	private List<String> marshalToCSV(AdaptrisMessage message, Map<String, String> jMap) throws ServiceException {
		List<String> record = new ArrayList<>();
		for (String header : header(message))
		{
			if (jMap.containsKey(header))
			{
				record.add(jMap.get(header));
			}
			else
			{
				record.add(""); // Add empty column to CSV
			}
		}
		return record;
	}

	/**
	 * Simple helper method, as the CSV header is better suited to
	 * being a list in most situations.
	 *
	 * @return A list of the header columns.
	 * @param message
	 */
	private List<String> header(AdaptrisMessage message) throws ServiceException {
		List<String> header = new ArrayList<>();
		try {
			for (String column : csvHeader.extract(message).trim().split(",\\s*"))
			{
				header.add(column);
			}
		} catch (InterlokException e) {
			log.error("Failed to access header value", e);
			throw new ServiceException(e);
		}
		return header;
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
