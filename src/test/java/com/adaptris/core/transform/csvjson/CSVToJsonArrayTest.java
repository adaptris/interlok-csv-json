package com.adaptris.core.transform.csvjson;

import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ServiceException;
import com.jayway.jsonpath.ReadContext;

public class CSVToJsonArrayTest extends CsvBaseCase {

  public CSVToJsonArrayTest(String name) {
    super(name);
  }

  public void testService() throws Exception {
    CSVToJsonArray service = new CSVToJsonArray();
    AdaptrisMessage msg = AdaptrisMessageFactory.getDefaultInstance().newMessage(CSV_INPUT);
    execute(service, msg);
    assertNotNull(msg.getContent());
    System.err.println(msg.getContent());
    ReadContext ctx = parse(msg);
    assertEquals("alice", ctx.read("$[0].firstname"));
    assertEquals("bob", ctx.read("$[1].firstname"));
    assertEquals("carol", ctx.read("$[2].firstname"));
  }


  public void testBrokenInput() throws Exception {
    CSVToJsonArray service = createForTests();
    AdaptrisMessage msg = new BrokenMessageFactory(WhenToBreak.INPUT).newMessage(CSV_INPUT);
    try {
      execute(service, msg);
      fail();
    } catch (ServiceException expected) {

    }
  }

  public void testBrokenOutput() throws Exception {
    CSVToJsonArray service = createForTests();
    AdaptrisMessage msg = new BrokenMessageFactory(WhenToBreak.OUTPUT).newMessage(CSV_INPUT);
    try {
      execute(service, msg);
      fail();
    } catch (ServiceException expected) {

    }
  }


  @Override
  protected CSVToJsonArray createForTests() {
    return new CSVToJsonArray();
  }

}
