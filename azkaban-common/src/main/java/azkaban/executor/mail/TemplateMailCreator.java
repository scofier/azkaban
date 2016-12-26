/*
 * Copyright 2012 LinkedIn Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package azkaban.executor.mail;

import java.io.StringWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;

import azkaban.executor.ExecutableFlow;
import azkaban.executor.ExecutionOptions;
import azkaban.executor.ExecutionOptions.FailureAction;
import azkaban.utils.EmailMessage;
import azkaban.utils.Emailer;
import azkaban.utils.Utils;

public class TemplateMailCreator extends DefaultMailCreator {
  public static final String DEFAULT_MAIL_CREATOR = "templatMail";

  private static MailCreator defaultCreator;
  
  private static VelocityEngine engine = new VelocityEngine();

  private static final DateFormat DATE_FORMATTER = new SimpleDateFormat(
      "yyyy/MM/dd HH:mm:ss z");


  static {
    defaultCreator = new TemplateMailCreator();
    registerCreator(DefaultMailCreator.DEFAULT_MAIL_CREATOR, defaultCreator);
  }

  @Override
  public boolean createFirstErrorMessage(ExecutableFlow flow,
      EmailMessage message, String azkabanName, String scheme,
      String clientHostname, String clientPortNumber, String... vars) {

    ExecutionOptions option = flow.getExecutionOptions();
    List<String> emailList = option.getFailureEmails();
    int execId = flow.getExecutionId();

    if (emailList != null && !emailList.isEmpty()) {
      message.addAllToAddress(emailList);
      message.setMimeType("text/html");
      message.setSubject("Flow '" + flow.getFlowId() + "' has encountered a failure on "
          + azkabanName);

          
          message.setBody(formatEmailTemplate(flow, "success"));
          return true;

    }

    return false;
  }

  @Override
  public boolean createErrorEmail(ExecutableFlow flow, EmailMessage message,
      String azkabanName, String scheme, String clientHostname,
      String clientPortNumber, String... vars) {

    ExecutionOptions option = flow.getExecutionOptions();

    List<String> emailList = option.getFailureEmails();
    int execId = flow.getExecutionId();

    if (emailList != null && !emailList.isEmpty()) {
      message.addAllToAddress(emailList);
      message.setMimeType("text/html");
      message.setSubject("Flow '" + flow.getFlowId() + "' has failed on "
          + azkabanName);

          
          message.setBody(formatEmailTemplate(flow, "success"));
      return true;
    }
    return false;
  }

  @Override
  public boolean createSuccessEmail(ExecutableFlow flow, EmailMessage message,
      String azkabanName, String scheme, String clientHostname,
      String clientPortNumber, String... vars) {

    ExecutionOptions option = flow.getExecutionOptions();
    List<String> emailList = option.getSuccessEmails();

    int execId = flow.getExecutionId();

    if (emailList != null && !emailList.isEmpty()) {
      message.addAllToAddress(emailList);
      message.setMimeType("text/html");
      

      message.setSubject("Flow '" + flow.getFlowId() + "' has succeeded on "
          + azkabanName);
      
      message.setBody(formatEmailTemplate(flow, "success"));
      return true;

    }
    return false;
  }
  
  private static String formatEmailTemplate(ExecutableFlow flow, String type) {
	  
	  StringWriter stringWriter = new StringWriter();
	  VelocityContext context = new VelocityContext();
	  
	  context.put("flow", flow);
	  engine.evaluate(context, stringWriter, "FormatEmailTemplate", "<b>aaaa=${flow.id}=aaa</b>");
	  
	  
	  return stringWriter.getBuffer().toString();
  }

  private static String convertMSToString(long timeInMS) {
    if (timeInMS < 0) {
      return "N/A";
    } else {
      return DATE_FORMATTER.format(new Date(timeInMS));
    }
  }
}
