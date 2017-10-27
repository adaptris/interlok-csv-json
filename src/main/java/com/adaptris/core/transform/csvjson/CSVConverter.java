package com.adaptris.core.transform.csvjson;

import javax.validation.constraints.NotNull;

import com.adaptris.annotation.AutoPopulated;
import com.adaptris.annotation.InputFieldDefault;
import com.adaptris.core.CoreException;
import com.adaptris.core.ServiceImp;
import com.adaptris.core.util.Args;
import com.adaptris.csv.BasicPreferenceBuilder;
import com.adaptris.csv.PreferenceBuilder;

public abstract class CSVConverter extends ServiceImp {

  @NotNull
  @AutoPopulated
  @InputFieldDefault(value = "csv-basic-preference-builder")
  private PreferenceBuilder preferenceBuilder;

  public CSVConverter() {
    setPreferenceBuilder(new BasicPreferenceBuilder(BasicPreferenceBuilder.Style.STANDARD_PREFERENCE));
  }


  @Override
  public void prepare() throws CoreException {}

  @Override
  protected void initService() throws CoreException {}

  @Override
  protected void closeService() {}

  /**
   * @return the formatBuilder
   */
  public PreferenceBuilder getPreferenceBuilder() {
    return preferenceBuilder;
  }

  /**
   * @param prefs the CSV Preferences to set
   */
  public void setPreferenceBuilder(PreferenceBuilder prefs) {
    this.preferenceBuilder = Args.notNull(prefs, "preference-builder");
  }
}
