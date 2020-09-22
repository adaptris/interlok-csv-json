package com.adaptris.core.transform.csvjson;

import com.adaptris.annotation.Removal;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceException;
import com.adaptris.core.util.LoggingHelper;

/**
 * Transform a CSV document into a JSON Array.
 * 
 * @deprecated since 3.11.1 poorly named class since we support json-lines as well, so use
 *             {@link CSVToJson} instead.
 */
@Deprecated
@Removal(version = "4.0.0")
public class CSVToJsonArray extends CSVConverter {

  private transient boolean warningLogged = false;

  @Override
  public void prepare() throws CoreException {
    LoggingHelper.logWarning(warningLogged, () -> warningLogged = true,
        "{} is deprecated use 'csv-json' with JSON_ARRAY instead",
        this.getClass().getCanonicalName());
  }

  @Override
  public void doService(AdaptrisMessage msg) throws ServiceException {
    new CsvJsonArrayTransformer(getPreferenceBuilder()).transform(msg);
  }
}
