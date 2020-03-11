package com.adaptris.core.transform.csvjson;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import org.junit.Test;
import com.adaptris.core.AdaptrisMessage;
import com.adaptris.core.AdaptrisMessageFactory;
import com.adaptris.core.ServiceException;
import com.jayway.jsonpath.ReadContext;

public class CSVToJsonArrayTest extends CsvBaseCase {
  @Override
  public boolean isAnnotatedForJunit4() {
    return true;
  }
  @Test
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


  @Test
  public void testBrokenInput() throws Exception {
    CSVToJsonArray service = createForTests();
    AdaptrisMessage msg = new BrokenMessageFactory(WhenToBreak.INPUT).newMessage(CSV_INPUT);
    try {
      execute(service, msg);
      fail();
    } catch (ServiceException expected) {

    }
  }

  @Test
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
