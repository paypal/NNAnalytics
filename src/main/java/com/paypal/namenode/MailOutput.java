/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.paypal.namenode;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import javax.mail.Message;
import javax.mail.Multipart;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import org.apache.hadoop.hdfs.server.namenode.NNLoader;
import org.apache.hadoop.hdfs.server.namenode.QueryEngine;

class MailOutput {

  private static final String MAIL_CONTENT_TEXT_HTML = "text/html";
  private static final String MAIL_SMTP_HOST = "mail.smtp.host";

  static void check(String emailConditionsStr, long value, NNLoader nnLoader) throws IOException {
    QueryEngine qEngine = nnLoader.getQueryEngine();
    List<Function<Long, Boolean>> comparisons = qEngine.createComparisons(emailConditionsStr);
    boolean shouldEmail = qEngine.check(comparisons, value);
    if (!shouldEmail) {
      throw new IOException("Failed to meet requirements for email.");
    }
  }

  static void check(
      String emailConditionsStr,
      Map<String, Long> histogram,
      Set<String> highlightKeys,
      NNLoader nnLoader)
      throws IOException {
    QueryEngine qEngine = nnLoader.getQueryEngine();
    List<Function<Long, Boolean>> comparisons = qEngine.createComparisons(emailConditionsStr);
    boolean shouldEmail = false;
    for (Map.Entry<String, Long> entry : histogram.entrySet()) {
      boolean columnCheck = qEngine.check(comparisons, entry.getValue());
      if (columnCheck) {
        shouldEmail = true;
        highlightKeys.add(entry.getKey());
      }
    }
    if (!shouldEmail) {
      throw new IOException("Failed to meet requirements for email.");
    }
  }

  static void write(
      String subject,
      Map<String, Long> histogram,
      Set<String> highlightKeys,
      String mailHost,
      String[] emailTo,
      String[] emailCC,
      String emailFrom)
      throws Exception {
    write(
        subject,
        convertHistogramToHtml(histogram, highlightKeys),
        mailHost,
        emailTo,
        emailCC,
        emailFrom);
  }

  static void write(
      String subject,
      String html,
      String mailHost,
      String[] emailTo,
      String[] emailCC,
      String emailFrom)
      throws Exception {

    InternetAddress[] to = new InternetAddress[emailTo.length];
    for (int i = 0; i < emailTo.length && i < to.length; i++) {
      to[i] = new InternetAddress(emailTo[i]);
    }

    InternetAddress[] cc = null;
    if (emailCC != null) {
      cc = new InternetAddress[emailCC.length];
      for (int i = 0; i < emailCC.length && i < cc.length; i++) {
        cc[i] = new InternetAddress(emailCC[i]);
      }
    }

    // Sender's email ID needs to be mentioned
    InternetAddress from = new InternetAddress(emailFrom);

    // Get system properties
    Properties properties = System.getProperties();

    // Setup mail server
    properties.setProperty(MAIL_SMTP_HOST, mailHost);

    // Get the Session object.
    Session session = Session.getInstance(properties);

    // Create a default MimeMessage object.
    MimeMessage message = new MimeMessage(session);

    // Set From: header field of the header.
    message.setFrom(from);

    // Set To: header field of the header.
    message.addRecipients(Message.RecipientType.TO, to);
    if (cc != null && cc.length != 0) {
      message.addRecipients(Message.RecipientType.CC, cc);
    }

    // Set Subject: header field
    message.setSubject(subject);

    // Send the actual HTML message, as big as you like
    MimeBodyPart bodyHtml = new MimeBodyPart();
    bodyHtml.setContent(html, MAIL_CONTENT_TEXT_HTML);

    Multipart multipart = new MimeMultipart();
    multipart.addBodyPart(bodyHtml);

    message.setContent(multipart);

    // Send message
    Transport.send(message);
  }

  private static String convertHistogramToHtml(
      Map<String, Long> histogram, Set<String> highlightKeys) {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, Long> entry : histogram.entrySet()) {
      if (highlightKeys.contains(entry.getKey())) {
        sb.append("<b>");
        sb.append(entry.getKey());
        sb.append("=");
        sb.append(entry.getValue());
        sb.append("</b>");
        sb.append("<br />");
      } else {
        sb.append(entry.getKey());
        sb.append("=");
        sb.append(entry.getValue());
        sb.append("<br />");
      }
    }
    return sb.toString();
  }
}
