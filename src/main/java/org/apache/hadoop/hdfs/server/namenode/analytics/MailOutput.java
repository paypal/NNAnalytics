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

package org.apache.hadoop.hdfs.server.namenode.analytics;

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
import org.apache.hadoop.hdfs.server.namenode.NameNodeLoader;
import org.apache.hadoop.hdfs.server.namenode.QueryEngine;

public class MailOutput {

  private static final String MAIL_CONTENT_TEXT_HTML = "text/html";
  private static final String MAIL_SMTP_HOST = "mail.smtp.host";

  /**
   * Check whether to send email or not against specific value.
   *
   * @param emailConditionsStr the conditions for whether to email
   * @param value the value ot compare against
   * @param nameNodeLoader the NameNodeLoader
   * @throws IOException if email requirements are not met
   */
  public static void check(String emailConditionsStr, long value, NameNodeLoader nameNodeLoader)
      throws IOException {
    QueryEngine queryEngine = nameNodeLoader.getQueryEngine();
    List<Function<Long, Boolean>> comparisons = queryEngine.createComparisons(emailConditionsStr);
    boolean shouldEmail = queryEngine.check(comparisons, value);
    if (!shouldEmail) {
      throw new IOException("Failed to meet requirements for email.");
    }
  }

  /**
   * Check whether to send email histogram or not against specific criteria.
   *
   * @param emailConditionsStr the conditions for whether to email
   * @param histogram the histogram to compare against
   * @param highlightKeys the keys to highlight in the email
   * @param nameNodeLoader the NameNodeLoader
   * @throws IOException if email requirements are not met
   */
  public static void check(
      String emailConditionsStr,
      Map<String, Long> histogram,
      Set<String> highlightKeys,
      NameNodeLoader nameNodeLoader)
      throws IOException {
    QueryEngine queryEngine = nameNodeLoader.getQueryEngine();
    List<Function<Long, Boolean>> comparisons = queryEngine.createComparisons(emailConditionsStr);
    boolean shouldEmail = false;
    for (Map.Entry<String, Long> entry : histogram.entrySet()) {
      boolean columnCheck = queryEngine.check(comparisons, entry.getValue());
      if (columnCheck) {
        shouldEmail = true;
        highlightKeys.add(entry.getKey());
      }
    }
    if (!shouldEmail) {
      throw new IOException("Failed to meet requirements for email.");
    }
  }

  /**
   * Write and send an email.
   *
   * @param subject email subject
   * @param histogram histogram output
   * @param highlightKeys histogram keys to highlight / bold in email
   * @param mailHost email host
   * @param emailTo email to address
   * @param emailCc email cc addresses
   * @param emailFrom email from address
   * @throws Exception if email fails to send
   */
  public static void write(
      String subject,
      Map<String, Long> histogram,
      Set<String> highlightKeys,
      String mailHost,
      String[] emailTo,
      String[] emailCc,
      String emailFrom)
      throws Exception {
    write(
        subject,
        convertHistogramToHtml(histogram, highlightKeys),
        mailHost,
        emailTo,
        emailCc,
        emailFrom);
  }

  /**
   * Write and send an email.
   *
   * @param subject email subject
   * @param html the exact html to send out via email
   * @param mailHost email host
   * @param emailTo email to address
   * @param emailCc email cc addresses
   * @param emailFrom email from address
   * @throws Exception if email fails to send
   */
  public static void write(
      String subject,
      String html,
      String mailHost,
      String[] emailTo,
      String[] emailCc,
      String emailFrom)
      throws Exception {

    InternetAddress[] to = new InternetAddress[emailTo.length];
    for (int i = 0; i < emailTo.length && i < to.length; i++) {
      to[i] = new InternetAddress(emailTo[i]);
    }

    InternetAddress[] cc = null;
    if (emailCc != null) {
      cc = new InternetAddress[emailCc.length];
      for (int i = 0; i < emailCc.length && i < cc.length; i++) {
        cc[i] = new InternetAddress(emailCc[i]);
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

    // INodeSet From: header field of the header.
    message.setFrom(from);

    // INodeSet To: header field of the header.
    message.addRecipients(Message.RecipientType.TO, to);
    if (cc != null && cc.length != 0) {
      message.addRecipients(Message.RecipientType.CC, cc);
    }

    // INodeSet Subject: header field
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
