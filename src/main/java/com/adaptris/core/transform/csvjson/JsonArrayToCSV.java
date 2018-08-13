package com.adaptris.core.transform.csvjson;

import java.util.ArrayList;
import java.util.Map;

import com.adaptris.annotation.*;
import com.adaptris.core.util.Args;
import com.adaptris.validation.constraints.BooleanExpression;
import org.supercsv.io.CsvListWriter;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ServiceException;
import com.adaptris.core.json.JsonUtil;
import com.adaptris.core.services.splitter.json.LargeJsonArraySplitter;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.core.util.LoggingHelper;
import com.thoughtworks.xstream.annotations.XStreamAlias;


/**
 * Transfrom a JSON array into a CSV.
 * 
 * <p>
 * Takes a JSON array and renders it as a CSV document:
 * <pre>
 * {@code 
 * [
 *   { "firstname":"alice", "lastname":"smith", "dob":"2017-01-01" },
 *   { "firstname":"bob", "lastname":"smith", "dob":"2017-01-02" },
 *   { "firstname":"carol", "lastname":"smith", "dob":"2017-01-03" }
 * ]}
 * </pre>
 * will effectively render as
 * <pre>
 * {@code
 * firstname, lastname, dob
 * "alice","smith", "2017-01-01"
 * "bob", "smith", "2017-01-02"
 * "carol","smith", "2017-01-03"
 * }
 * </pre>
 * </p>
 * <p>
 * nested JSON objects will be rendered as strings before being passed into the appropriate statement; so
 * {@code { "firstname":"alice", "lastname":"smith", "address": { "address" : "Buckingham Palace", "postcode":"SW1A 1AA"}}} still
 * only be 3 CSV columns the address column will be {@code "{ "address" : "Buckingham Palace", "postcode":"SW1A 1AA"}"}
 * </p>
 * *
 * </p>
 * 
 * @config json-to-csv
 *
 */
@AdapterComponent
@ComponentProfile(summary = "Transfrom a JSON Array into a CSV", tag = "service,csv,json")
@XStreamAlias("json-to-csv")
public class JsonArrayToCSV extends CSVConverter {

  @AdvancedConfig
  @InputFieldHint(expression = true)
  @InputFieldDefault(value = "true")
  @BooleanExpression
  private String includeHeader;

  public JsonArrayToCSV() {
    super();
  }


  @Override
  public void doService(AdaptrisMessage msg) throws ServiceException {
    try {
      log.trace("Beginning doService in {}", LoggingHelper.friendlyName(this));
      // Use the already existing LargeJsonArraySplitter, but force it with a default-mf
      LargeJsonArraySplitter splitter =
          new LargeJsonArraySplitter().withMessageFactory(AdaptrisMessageFactory.getDefaultInstance());
      boolean first = true;
      try (CsvListWriter csvWriter = new CsvListWriter(msg.getWriter(), getPreferenceBuilder().build())) {
        for (AdaptrisMessage m : splitter.splitMessage(msg)) {
          Map<String, String> json = JsonUtil.mapifyJson(m);
          if (first && includeHeader(msg)) {
            csvWriter.writeHeader(json.keySet().toArray(new String[0]));
            first = false;
          }
          csvWriter.write(new ArrayList<String>(json.values()));
        }
      }
    } catch (Exception e) {
      throw ExceptionHelper.wrapServiceException(e);
    } finally {
    }
  }

  public String getIncludeHeader() {
    return includeHeader;
  }

  public void setIncludeHeader(String includeHeader) {
    this.includeHeader = Args.notEmpty(includeHeader, "includeHeader");
  }

  private boolean includeHeader(AdaptrisMessage msg){
    return getIncludeHeader() != null  ? Boolean.valueOf(msg.resolve(getIncludeHeader())) : true;
  }
}
