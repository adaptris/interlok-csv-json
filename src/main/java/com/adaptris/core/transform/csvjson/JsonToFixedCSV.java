package com.adaptris.core.transform.csvjson;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.BooleanUtils;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.annotation.InputFieldHint;
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
import com.adaptris.core.util.ExceptionHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;

/**
 * Marshall the JSON doc to CSV but maintain the field ordering. We do currently have JSON to CSV services (interlok-csv-json), but
 * we need a custom service for this.
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
@ComponentProfile(summary = "Transform a JSON document to CSV with a fixed header", tag = "service,transform,json,csv",
    recommended = {JsonArraySplitter.class, JsonObjectSplitter.class, LargeJsonArraySplitter.class, LineCountSplitter.class})
public class JsonToFixedCSV extends ServiceImp {

  @NotBlank
  @Valid
  @InputFieldHint(expression = true)
  private String csvHeader = "";

  @Valid
  @InputFieldDefault(value = "true")
  private Boolean showHeader;

  @NotNull
  @Valid
  private MessageSplitterImp messageSplitter = new JsonObjectSplitter();

  public JsonToFixedCSV() {
    
  }
  
  public JsonToFixedCSV(String hdrs) {
    this();
    setCsvHeader(hdrs);
  }
  
  /**
   * Set the CSV header.
   *
   * @param csvHeader The CSV header row.
   */
  public void setCsvHeader(String csvHeader) {
    this.csvHeader = Args.notBlank(csvHeader, "CSV Header");
  }

  /**
   * Get the CSV header.
   *
   * @return The CSV header row.
   */
  public String getCsvHeader() {
    return csvHeader;
  }

  /**
   * Set whether the CSV header should be in the output.
   *
   * @param showHeader True if CSV header should be included.
   */
  public void setShowHeader(Boolean showHeader) {
    this.showHeader = showHeader;
  }

  /**
   * Get whether the CSV header is included in the output.
   *
   * @return True if CSV header is included.
   */
  public Boolean isShowHeader() {
    return showHeader;
  }

  /**
   * Get whether the CSV header is included in the output. Convention says you should use {@code}is...()} for boolean values, so go
   * use {@link JsonToFixedCSV#isShowHeader} instead. This method is provided in case XStream isn't that smart.
   *
   * @return True if CSV header are included.
   */
  public Boolean getShowHeader() {
    return isShowHeader();
  }

  protected boolean showHeaders() {
    return BooleanUtils.toBooleanDefaultIfNull(getShowHeader(), true);
  }

  /**
   * Set the splitter to use to divvy up the original message if it's particularly large.
   *
   * @param messageSplitter The message splitter.
   */
  public void setMessageSplitter(MessageSplitterImp messageSplitter) {
    this.messageSplitter = Args.notNull(messageSplitter, "Message Splitter");
  }

  /**
   * Get the splitter used to divvy up the original message if it's particularly large.
   *
   * @return The message splitter.
   */
  public MessageSplitter getMessageSplitter() {
    return messageSplitter;
  }

  /**
   * {@inheritDoc}.
   */
  @Override
  public void doService(AdaptrisMessage message) throws ServiceException {
    log.trace("Starting JSON to CSV transformation with" + (showHeaders() ? "" : "out") + " header");
    List<String> headers = header(message);
    try (CSVPrinter csv = new CSVPrinter(message.getWriter(), CSVFormat.DEFAULT)) {
      if (showHeaders()) {
        csv.printRecord(headers);
      }

      messageSplitter.setMessageFactory(AdaptrisMessageFactory.getDefaultInstance());
      try (CloseableIterable<AdaptrisMessage> splitMessages =
          CloseableIterable.ensureCloseable(messageSplitter.splitMessage(message))) {
        if (messageSplitter.getClass().equals(LargeJsonArraySplitter.class)
            || messageSplitter.getClass().equals(JsonArraySplitter.class)) {
          log.trace("Split JSON array into series of messages");
          for (AdaptrisMessage splitMessage : splitMessages) {
            Map<String, String> jMap = JsonUtil.mapifyJson(splitMessage);
            log.trace("JSON object has " + jMap.size() + " keys");
            csv.printRecord(marshalToCSV(jMap, headers));
          }
        } else {
          // handles a JSON object but not a JSON array
          log.trace("Split JSON message into series of messages ");
          Map<String, String> jMap = new HashMap<>();
          for (AdaptrisMessage splitMessage : splitMessages) {
            Map<String, String> map = JsonUtil.mapifyJson(splitMessage);
            log.trace("JSON object has another " + map.size() + " keys");
            jMap.putAll(map);
          }
          csv.printRecord(marshalToCSV(jMap, headers));
        }
      }
    } catch (Exception e) {
      throw ExceptionHelper.wrapServiceException("Could not parse JSON nor generate CSV data", e);
    } finally {
      log.trace("Finished JSON to CSV transformation");
    }
  }

  /**
   * Marshal part of a large JSON array to CSV.
   *
   * @param jMap The map of this particular JSON object.
   *
   * @return The CSV row as a list of Strings.
   */
  private List<String> marshalToCSV(Map<String, String> jMap, List<String> headers) {
    List<String> record = new ArrayList<>();
    for (String header : headers) {
      if (jMap.containsKey(header)) {
        record.add(jMap.get(header));
      } else {
        record.add(""); // Add empty column to CSV
      }
    }
    return record;
  }

  /**
   * Simple helper method, as the CSV header is better suited to being a list in most situations.
   *
   * @return A list of the header columns.
   */
  private List<String> header(AdaptrisMessage msg) {
    String hdr = msg.resolve(getCsvHeader());
    List<String> header = new ArrayList<>();
    for (String column : hdr.trim().split(",\\s*")) {
      header.add(column);
    }
    return header;
  }

  /**
   * {@inheritDoc}.
   */
  @Override
  protected void initService() throws CoreException {
    Args.notBlank(getCsvHeader(), "csv-header");
  }

  /**
   * {@inheritDoc}.
   */
  @Override
  protected void closeService() {
    /* not implemented */
  }

  /**
   * {@inheritDoc}.
   */
  @Override
  public void prepare() throws CoreException {
    /* not implemented */
  }
}
