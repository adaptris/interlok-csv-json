package com.adaptris.core.transform.csvjson;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.supercsv.io.CsvListWriter;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.AdvancedConfig;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.annotation.DisplayOrder;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.annotation.InputFieldHint;
import com.adaptris.annotation.Removal;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.json.JsonUtil;
import com.adaptris.core.services.splitter.LineCountSplitter;
import com.adaptris.core.services.splitter.MessageSplitterImp;
import com.adaptris.core.services.splitter.json.JsonArraySplitter;
import com.adaptris.core.services.splitter.json.JsonObjectSplitter;
import com.adaptris.core.services.splitter.json.JsonProvider.JsonObjectProvider;
import com.adaptris.core.services.splitter.json.JsonProvider.JsonStyle;
import com.adaptris.core.services.splitter.json.LargeJsonArraySplitter;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.CloseableIterable;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.core.util.LoggingHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Transfrom a JSON array into a CSV with only the specified fields.
 * 
 * @config json-to-fixed-csv-service
 * 
 */
@XStreamAlias("json-to-fixed-csv-service")
@AdapterComponent
@ComponentProfile(summary = "Transform a JSON document to CSV with fixed fields", tag = "service,transform,json,csv")
@DisplayOrder(order = {"csvHeader", "includeHeader", "jsonStyle", "preferenceBuilder"})
@NoArgsConstructor
public class JsonToFixedCSV extends JsonArrayToCSV {

  private transient boolean headerWarning = false;
  private transient boolean splitterWarning = false;

  private static final Map<Class, JsonStyle> SPLITTER_STYLE;

  static {
    // Map the supported splitters into the various possible styles.
    Map<Class, JsonStyle> map = new HashMap<>();
    map.put(JsonArraySplitter.class, JsonStyle.JSON_ARRAY);
    map.put(LargeJsonArraySplitter.class, JsonStyle.JSON_ARRAY);
    map.put(JsonObjectSplitter.class, JsonStyle.JSON_OBJECT);
    map.put(LineCountSplitter.class, JsonStyle.JSON_LINES);
    SPLITTER_STYLE = Collections.unmodifiableMap(map);
  }

  /**
   * The CSV Header.
   * 
   */
  @NotBlank
  @InputFieldHint(expression = true)
  @Getter
  @Setter
  private String csvHeader = "";

  /**
   * Whether or not to emit the header in the resulting document.
   * 
   */
  @Valid
  @InputFieldDefault(value = "true")
  @Getter
  @Setter
  @Deprecated
  @Removal(version = "3.11", message = "use include-header instead")
  private Boolean showHeader;

  /**
   * The splitter used to generate each CSV row.
   */
  @Valid
  @Getter
  @Setter
  @AdvancedConfig(rare = true)
  @Deprecated
  @Removal(version = "3.11.0", message = "Use JsonStyle instead")
  private MessageSplitterImp messageSplitter = null;

  public JsonToFixedCSV(String hdrs) {
    this();
    setCsvHeader(hdrs);
  }
  
  @Override
  protected boolean includeHeader(AdaptrisMessage msg) {
    if (getShowHeader() != null) {
      LoggingHelper.logWarning(headerWarning, () -> {
        headerWarning = true;
      }, "show-headers is deprecated, use include-header instead");
      return BooleanUtils.toBooleanDefaultIfNull(getShowHeader(), true);

    }
    return super.includeHeader(msg);
  }

  @Override
  public void doService(AdaptrisMessage msg) throws ServiceException {

    try {
      boolean includeHeaders = includeHeader(msg);
      log.trace("Starting JSON to CSV transformation with" + (includeHeaders ? "" : "out") + " header");
      List<String> headers = header(msg);
      boolean first = true;
      JsonObjectProvider style = jsonStyle();
      try (CsvListWriter csvWriter = new CsvListWriter(msg.getWriter(), getPreferenceBuilder().build());
          CloseableIterable<AdaptrisMessage> splitMsgs = CloseableIterable.ensureCloseable(style.createIterator(msg))) {
        if (includeHeaders) {
          csvWriter.writeHeader(headers.toArray(new String[0]));
        }

        // If it's a JSON Object split then effectively each split message is part of the same "row"
        // so it's a single row output + a possible header.
        if (style == JsonStyle.JSON_OBJECT) {
          Map<String, String> jMap = new HashMap<>();
          for (AdaptrisMessage splitMessage : splitMsgs) {
            Map<String, String> map = JsonUtil.mapifyJson(splitMessage);
            jMap.putAll(map);
          }
          csvWriter.write(toRecord(jMap, headers));
        } else {
          for (AdaptrisMessage splitMessage : splitMsgs) {
            Map<String, String> jMap = JsonUtil.mapifyJson(splitMessage);
            log.trace("JSON object has " + jMap.size() + " keys");
            csvWriter.write(toRecord(jMap, headers));
          }
        }
      }
    } catch (Exception e) {
      throw ExceptionHelper.wrapServiceException("Could not parse JSON nor generate CSV data", e);
    }
  }


  /**
   * Grabs the values from a map, and makes sure it's in the same order as the headers.
   *
   * @return The CSV row as a list of Strings.
   */
  private List<String> toRecord(Map<String, String> jMap, List<String> headers) {
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
    return new ArrayList<>(Arrays.asList(hdr.trim().split(",")));
  }

  @Override
  public void prepare() throws CoreException {
    super.prepare();
    Args.notBlank(getCsvHeader(), "csv-header");
  }

  // if message-splitter is not null then it might be a splitter that maps directly onto a "style"
  // JSON_OBJECT / JSON_ARRAY / JSON_LINES
  // Otherwise it's a custom splitter which means we have to implement the functional JsonObjectProvider
  // interface
  @Override
  protected JsonObjectProvider jsonStyle() {
    if (getMessageSplitter() != null) {
      LoggingHelper.logWarning(splitterWarning, () -> {
        splitterWarning = true;
      }, "message-splitter is deprecated, use a JsonStyle instead");
      JsonObjectProvider style = SPLITTER_STYLE.get(getMessageSplitter().getClass());
      // Might be a LargeJsonArrayPathSplitter (crazy talk if you ask me).
      if (style == null) {
        style = (msg) -> {
          MessageSplitterImp imp = getMessageSplitter();
          imp.setMessageFactory(AdaptrisMessageFactory.getDefaultInstance());
          return CloseableIterable.ensureCloseable(imp.splitMessage(msg));
        };
      }
      return Args.notNull(style, "json style");
    }
    return ObjectUtils.defaultIfNull(getJsonStyle(), JsonStyle.JSON_ARRAY);
  }

}
