package com.adaptris.core.transform.csvjson;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.LinkedHashMap;
import java.util.Map;
import org.supercsv.io.CsvMapReader;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.util.Util;
import com.adaptris.annotation.AdapterComponent;
import com.adaptris.annotation.ComponentProfile;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.ServiceException;
import com.adaptris.core.util.Args;
import com.adaptris.core.util.ExceptionHelper;
import com.adaptris.core.util.LoggingHelper;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import lombok.NoArgsConstructor;

/**
 * Transform a CSV document into a JSON Array.
 * 
 * <p>
 * Takes a CSV document and renders as it as JSON:
 * <pre>
 * {@code
 * firstname, lastname, dob
 * "alice","smith", "2017-01-01"
 * "bob", "smith", "2017-01-02"
 * "carol","smith", "2017-01-03"
 * }
 * </pre>
 * will effectively render as
 * <pre>
 * {@code 
 * [
 *   { "firstname":"alice", "lastname":"smith", "dob":"2017-01-01" },
 *   { "firstname":"bob", "lastname":"smith", "dob":"2017-01-02" },
 *   { "firstname":"carol", "lastname":"smith", "dob":"2017-01-03" }
 * ]}
 * </pre>
 * </p>
 * 
 * @config csv-to-json
 *
 */
@AdapterComponent
@ComponentProfile(summary = "Transfrom CSV into JSON", tag = "service,csv,json")
@XStreamAlias("csv-to-json")
@NoArgsConstructor
public class CSVToJsonArray extends CSVConverter {


  @Override
  public void doService(AdaptrisMessage msg) throws ServiceException {
    log.trace("Beginning doService in {}", LoggingHelper.friendlyName(this));
    ObjectMapper mapper = new ObjectMapper();
    try (Reader reader = msg.getReader();
        CsvMapReader csvReader = new PredictableMapReader(reader, getPreferenceBuilder().build());
        Writer w = msg.getWriter();
        JsonGenerator generator = mapper.getFactory().createGenerator(w).useDefaultPrettyPrinter()) {
      String[] hdrs = csvReader.getHeader(true);
      generator.writeStartArray();
      for (Map<String, String> row; (row = csvReader.read(hdrs)) != null;) {
        generator.writeObject(row);
      }
      generator.writeEndArray();;
    } catch (Exception e) {
      throw ExceptionHelper.wrapServiceException(e);
    } finally {
    }
  }

  // Copy of CsvMapReader but use LinkedHashMap for predictable iteration.
  private class PredictableMapReader extends CsvMapReader {
    public PredictableMapReader(final Reader reader, final CsvPreference preferences) {
      super(reader, preferences);
    }

    @Override
    public Map<String, String> read(final String... nameMapping) throws IOException {
      Args.notNull(nameMapping, "mappings");
      if (readRow()) {
        final Map<String, String> destination = new LinkedHashMap<String, String>();
        Util.filterListToMap(destination, nameMapping, getColumns());
        return destination;
      }
      return null; // EOF
    }
  }
}
