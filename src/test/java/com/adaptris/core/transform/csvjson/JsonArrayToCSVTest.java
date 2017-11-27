package com.adaptris.core.transform.csvjson;

import java.io.InputStream;
import java.util.List;

import org.apache.commons.io.IOUtils;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ServiceException;
import com.jayway.jsonpath.ReadContext;

public class JsonArrayToCSVTest extends CsvBaseCase {

  public JsonArrayToCSVTest(String name) {
    super(name);
  }

  public void testService() throws Exception {
    JsonArrayToCSV service = new JsonArrayToCSV();
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(JSON_ARRAY_INPUT);
    execute(service, msg);
    System.err.println(msg.getContent());
    try (InputStream in = msg.getInputStream()){
      List<String> lines = IOUtils.readLines(in);
      assertEquals(4, lines.size());
    }
  }

  public void testServiceRoundTrip() throws Exception {
    JsonArrayToCSV s1 = new JsonArrayToCSV();
    CSVToJsonArray s2 = new CSVToJsonArray();
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(JSON_ARRAY_INPUT);
    execute(s1, msg);
    execute(s2, msg);
    System.err.println(msg.getContent());
    ReadContext ctx = parse(msg);
    assertEquals("alice", ctx.read("$[0].firstname"));
    assertEquals("bob", ctx.read("$[1].firstname"));
    assertEquals("carol", ctx.read("$[2].firstname"));
  }



  public void testBrokenInput() throws Exception {
    JsonArrayToCSV service = createForTests();
    AdaptrisMessage msg = new BrokenMessageFactory(WhenToBreak.INPUT).newMessage(JSON_ARRAY_INPUT);
    try {
      execute(service, msg);
      fail();
    } catch (ServiceException expected) {

    }
  }

  public void testBrokenOutput() throws Exception {
    JsonArrayToCSV service = createForTests();
    AdaptrisMessage msg = new BrokenMessageFactory(WhenToBreak.OUTPUT).newMessage(JSON_ARRAY_INPUT);
    try {
      execute(service, msg);
      fail();
    } catch (ServiceException expected) {

    }
  }


  @Override
  protected JsonArrayToCSV createForTests() {
    return new JsonArrayToCSV();
  }

}
