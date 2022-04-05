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

package org.apache.hadoop.hdfs.server.namenode.analytics.web;

import static org.apache.hadoop.hdfs.server.namenode.analytics.NameNodeAnalyticsHttpServer.NNA_APP_CONF;
import static org.apache.hadoop.hdfs.server.namenode.analytics.NameNodeAnalyticsHttpServer.NNA_CANCEL_REQUEST;
import static org.apache.hadoop.hdfs.server.namenode.analytics.NameNodeAnalyticsHttpServer.NNA_HSQL_DRIVER;
import static org.apache.hadoop.hdfs.server.namenode.analytics.NameNodeAnalyticsHttpServer.NNA_NN_LOADER;
import static org.apache.hadoop.hdfs.server.namenode.analytics.NameNodeAnalyticsHttpServer.NNA_OPERATION_SERVICE;
import static org.apache.hadoop.hdfs.server.namenode.analytics.NameNodeAnalyticsHttpServer.NNA_QUERY_LOCK;
import static org.apache.hadoop.hdfs.server.namenode.analytics.NameNodeAnalyticsHttpServer.NNA_RUNNING_OPERATIONS;
import static org.apache.hadoop.hdfs.server.namenode.analytics.NameNodeAnalyticsHttpServer.NNA_RUNNING_QUERIES;
import static org.apache.hadoop.hdfs.server.namenode.analytics.NameNodeAnalyticsHttpServer.NNA_SAVING_NAMESPACE;
import static org.apache.hadoop.hdfs.server.namenode.analytics.NameNodeAnalyticsHttpServer.NNA_SECURITY_CONTEXT;
import static org.apache.hadoop.hdfs.server.namenode.analytics.NameNodeAnalyticsHttpServer.NNA_USAGE_METRICS;

import com.google.common.base.Strings;
import com.sun.management.OperatingSystemMXBean;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.MalformedURLException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.server.namenode.Constants;
import org.apache.hadoop.hdfs.server.namenode.Constants.Endpoint;
import org.apache.hadoop.hdfs.server.namenode.Constants.Filter;
import org.apache.hadoop.hdfs.server.namenode.Constants.FilterOp;
import org.apache.hadoop.hdfs.server.namenode.Constants.Find;
import org.apache.hadoop.hdfs.server.namenode.Constants.Histogram;
import org.apache.hadoop.hdfs.server.namenode.Constants.HistogramOutput;
import org.apache.hadoop.hdfs.server.namenode.Constants.INodeSet;
import org.apache.hadoop.hdfs.server.namenode.Constants.Operation;
import org.apache.hadoop.hdfs.server.namenode.Constants.Sum;
import org.apache.hadoop.hdfs.server.namenode.Constants.Transform;
import org.apache.hadoop.hdfs.server.namenode.INode;
import org.apache.hadoop.hdfs.server.namenode.NameNodeLoader;
import org.apache.hadoop.hdfs.server.namenode.TransferFsImageWrapper;
import org.apache.hadoop.hdfs.server.namenode.analytics.ApplicationConfiguration;
import org.apache.hadoop.hdfs.server.namenode.analytics.Helper;
import org.apache.hadoop.hdfs.server.namenode.analytics.HistogramInvoker;
import org.apache.hadoop.hdfs.server.namenode.analytics.HistogramTwoLevelInvoker;
import org.apache.hadoop.hdfs.server.namenode.analytics.HsqlDriver;
import org.apache.hadoop.hdfs.server.namenode.analytics.MailOutput;
import org.apache.hadoop.hdfs.server.namenode.analytics.QueryChecker;
import org.apache.hadoop.hdfs.server.namenode.analytics.UsageMetrics;
import org.apache.hadoop.hdfs.server.namenode.analytics.security.SecurityContext;
import org.apache.hadoop.hdfs.server.namenode.analytics.sql.SqlParser;
import org.apache.hadoop.hdfs.server.namenode.operations.BaseOperation;
import org.apache.hadoop.hdfs.server.namenode.operations.Delete;
import org.apache.hadoop.hdfs.server.namenode.operations.SetReplication;
import org.apache.hadoop.hdfs.server.namenode.operations.SetStoragePolicy;
import org.apache.hadoop.hdfs.server.namenode.queries.BaseQuery;
import org.apache.hadoop.hdfs.server.namenode.queries.Histograms;
import org.apache.hadoop.hdfs.server.namenode.queries.Transforms;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authorize.AuthorizationException;
import org.apache.hadoop.util.Time;
import org.apache.http.HttpStatus;
import org.apache.http.entity.StringEntity;
import org.pac4j.core.exception.BadCredentialsException;
import org.pac4j.core.exception.HttpAction;

/** NameNode Analytics implementation. */
@Path("")
public class NamenodeAnalyticsMethods {

  public static final Log LOG = LogFactory.getLog(NamenodeAnalyticsMethods.class);

  private @Context ServletContext context;
  private @Context HttpServletRequest request;
  private @Context HttpServletResponse response;

  /** Handle fetching index.html at root. */
  @GET
  @Path("/")
  public Response getRoot() {

    final InputStream resource = context.getResourceAsStream(String.format("/%s", "index.html"));

    return null == resource
        ? Response.status(HttpStatus.SC_NOT_FOUND).build()
        : Response.ok().entity(resource).build();
  }

  /** Handle fetching static resources vs API. */
  @GET
  @Path("{path:.*}")
  public Response getPath(@PathParam("path") final String path) {

    final InputStream resource = context.getResourceAsStream(String.format("/%s", path));

    return null == resource
        ? Response.status(HttpStatus.SC_NOT_FOUND).build()
        : Response.ok().entity(resource).build();
  }

  /** LOGIN is used to log into authenticated web sessions. */
  @POST
  @Path("/login")
  @Produces(MediaType.TEXT_PLAIN)
  @Consumes(MediaType.APPLICATION_FORM_URLENCODED)
  public Response login(MultivaluedMap<String, String> formData) {
    try {
      before();
      final SecurityContext securityContext =
          (SecurityContext) context.getAttribute(NNA_SECURITY_CONTEXT);
      final UsageMetrics usageMetrics = (UsageMetrics) context.getAttribute(NNA_USAGE_METRICS);
      securityContext.login(request, response, formData);
      usageMetrics.userLoggedIn(securityContext, request);
      return Response.ok().build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /** LOGOUT is used to log out of authenticated web sessions. */
  @POST
  @Path("/logout")
  @Produces(MediaType.TEXT_PLAIN)
  public Response logout() {
    final SecurityContext securityContext =
        (SecurityContext) context.getAttribute(NNA_SECURITY_CONTEXT);
    final UsageMetrics usageMetrics = (UsageMetrics) context.getAttribute(NNA_USAGE_METRICS);
    try {
      before();
      securityContext.logout(request, response);
      usageMetrics.userLoggedOut(securityContext, request);
      return Response.ok().build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * CREDENTIALS endpoint is meant to showcase what authorizations the querying user possesses in
   * JSON form.
   */
  @GET
  @Path("/credentials")
  @Produces({MediaType.APPLICATION_JSON})
  public Response credentials() {
    final SecurityContext securityContext =
        (SecurityContext) context.getAttribute(NNA_SECURITY_CONTEXT);
    try {
      before();
      if (securityContext.isAuthenticationEnabled()) {
        return Response.ok(
                Helper.toJsonList(securityContext.getAccessLevels()), MediaType.APPLICATION_JSON)
            .build();
      } else {
        return Response.status(HttpStatus.SC_BAD_REQUEST).build();
      }
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /** ENDPOINTS endpoint is meant to showcase all available REST API endpoints in JSON list form. */
  @GET
  @Path("/endpoints")
  @Produces({MediaType.APPLICATION_JSON})
  public Response endpoints() {
    try {
      before();
      return Response.ok(Helper.toJsonList(Endpoint.values()), MediaType.APPLICATION_JSON).build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * HISTOGRAMS endpoint is meant to showcase the different types of histograms available in the
   * "&type=" parameter in JSON form.
   */
  @GET
  @Path("/histograms")
  @Produces({MediaType.APPLICATION_JSON})
  public Response histograms() {
    try {
      before();
      return Response.ok(Helper.toJsonList(Histogram.values()), MediaType.APPLICATION_JSON).build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * HISTOGRAMOUTPUTS endpoint is meant to showcase the different types of histogram output options
   * available in the "&histogramOutput=" parameter in JSON form. These are only applicable under
   * the /histogram endpoint.
   */
  @GET
  @Path("/histogramOutputs")
  @Produces({MediaType.APPLICATION_JSON})
  public Response histogramOutputs() {
    try {
      before();
      return Response.ok(Helper.toJsonList(HistogramOutput.values()), MediaType.APPLICATION_JSON)
          .build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * FILTERS endpoint is meant to showcase the different types of filters options available in the
   * "&filters=" parameter in JSON form. These are only applicable under the /divide, /filter, and
   * /histogram endpoints.
   */
  @GET
  @Path("/filters")
  @Produces({MediaType.APPLICATION_JSON})
  public Response filters() {
    try {
      before();
      return Response.ok(Helper.toJsonList(Filter.values()), MediaType.APPLICATION_JSON).build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * FINDS endpoint is meant to showcase the different types of find options available in the
   * "&find=" parameter in JSON form. These are only applicable under the /filter and /histogram
   * endpoints.
   */
  @GET
  @Path("/finds")
  @Produces({MediaType.APPLICATION_JSON})
  public Response finds() {
    try {
      before();
      return Response.ok(Helper.toJsonList(Find.values()), MediaType.APPLICATION_JSON).build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * TRANSFORMS endpoint is meant to showcase the different types of INode field transform options
   * available in the "&transformFields=" parameter in JSON form. These are only applicable under
   * the /histogram endpoints.
   */
  @GET
  @Path("/transforms")
  @Produces({MediaType.APPLICATION_JSON})
  public Response transforms() {
    try {
      before();
      return Response.ok(Helper.toJsonList(Transform.values()), MediaType.APPLICATION_JSON).build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * SETS endpoint is meant to showcase the different types of INode sets available in the "&set="
   * parameter in JSON form. These are only applicable under the /divide, /filter and /histogram
   * endpoints.
   */
  @GET
  @Path("/sets")
  @Produces({MediaType.APPLICATION_JSON})
  public Response sets() {
    try {
      before();
      return Response.ok(Helper.toJsonList(INodeSet.values()), MediaType.APPLICATION_JSON).build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * FILTEROPS endpoint is meant to showcase the different types of filter operations options
   * available in the "&filterOps=" parameter in JSON form. These are only applicable while the
   * "&filters" parameter is utilized. Certain filterOps are only applicable to matching types of
   * filters as well.
   */
  @GET
  @Path("/filterOps")
  @Produces({MediaType.APPLICATION_JSON})
  public Response filterOps() {
    try {
      before();
      return Response.ok(Helper.toJsonList(FilterOp.values()), MediaType.APPLICATION_JSON).build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * SUMS endpoint is meant to showcase the different types of summations available in JSON form.
   * These are only applicable under the /divide, /filter and /histogram endpoints. Under /divide
   * and /filter, the "&sum=" type dictates what to sum of the filtered set. Under /histogram, the
   * "&sum=" type dictates what the Y axis of the histogram represents.
   */
  @GET
  @Path("/sums")
  @Produces({MediaType.APPLICATION_JSON})
  public Response sums() {
    try {
      before();
      return Response.ok(Helper.toJsonList(Sum.values()), MediaType.APPLICATION_JSON).build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * OPERATIONS endpoint is meant to showcase the different types of operations available in JSON
   * form. These are only applicable under the /submitOperation endpoint. Under /submitOperation,
   * the "&operation=" type dictates what operation to do with the filtered set.
   */
  @GET
  @Path("/operations")
  @Produces({MediaType.APPLICATION_JSON})
  public Response operations() {
    try {
      before();
      return Response.ok(Helper.toJsonList(Operation.values()), MediaType.APPLICATION_JSON).build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /** QUERYGUARD endpoint is an admin-level endpoint meant to kill all running analysis queries. */
  @GET
  @Path("/queryGuard")
  @Produces({MediaType.TEXT_PLAIN})
  public Response queryGuard() {
    try {
      before();
      final AtomicBoolean cancelRequest = (AtomicBoolean) context.getAttribute(NNA_CANCEL_REQUEST);
      boolean guarding = !cancelRequest.get();
      cancelRequest.set(guarding);
      if (guarding) {
        return Response.ok(
                "Guarding against queries. All queries after current processing will be killed.",
                MediaType.TEXT_PLAIN)
            .build();
      } else {
        return Response.ok("All queries allowed.", MediaType.TEXT_PLAIN).build();
      }
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * METRICS endpoint is meant to return information on the users and the amount of queries they are
   * making.
   */
  @GET
  @Path("/metrics")
  @Produces({MediaType.APPLICATION_JSON})
  public Response metrics() {
    try {
      before();
      final UsageMetrics usageMetrics = (UsageMetrics) context.getAttribute(NNA_USAGE_METRICS);
      return Response.ok(usageMetrics.getUserMetricsJson(), MediaType.APPLICATION_JSON).build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * LOADINGSTATUS endpoint is meant to show the loading status of the NNA instance in JSON form.
   */
  @GET
  @Path("/loadingStatus")
  @Produces({MediaType.APPLICATION_JSON})
  public Response loadingStatus() {
    final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
    try {
      before();
      nnLoader.sendLoadingStatus(response);
      return Response.ok().build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * THREADS endpoint is meant to show the thread information about the NNA instance in PLAINTEXT
   * form. TODO: Convert the output to JSON form.
   */
  @GET
  @Path("/threads")
  @Produces({MediaType.TEXT_PLAIN})
  public Response threads() {
    try {
      before();
      StringWriter writer = new StringWriter();
      ThreadMXBean tb = ManagementFactory.getThreadMXBean();
      int threadsNew = 0;
      int threadsRunnable = 0;
      int threadsBlocked = 0;
      int threadsWaiting = 0;
      int threadsTimedWaiting = 0;
      int threadsTerminated = 0;
      long[] threadIds = tb.getAllThreadIds();
      for (ThreadInfo threadInfo : tb.getThreadInfo(threadIds, 0)) {
        if (threadInfo == null) {
          continue; // race protection
        }
        switch (threadInfo.getThreadState()) {
          case NEW:
            threadsNew++;
            break;
          case RUNNABLE:
            threadsRunnable++;
            break;
          case BLOCKED:
            threadsBlocked++;
            break;
          case WAITING:
            threadsWaiting++;
            break;
          case TIMED_WAITING:
            threadsTimedWaiting++;
            break;
          case TERMINATED:
            threadsTerminated++;
            break;
          default:
            break;
        }
      }
      writer.write(
          "New Threads: "
              + threadsNew
              + "\n"
              + "Runnable Threads: "
              + threadsRunnable
              + "\n"
              + "Blocked Threads: "
              + threadsBlocked
              + "\n"
              + "Waiting Threads: "
              + threadsWaiting
              + "\n"
              + "Timed-Waiting Threads: "
              + threadsTimedWaiting
              + "\n"
              + "Terminated Threads: "
              + threadsTerminated
              + "\n");
      return Response.ok(writer.toString(), MediaType.TEXT_PLAIN).build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * SYSTEM endpoint is meant to show the system resource usage of the NNA instance in PLAINTEXT
   * form. TODO: Convert the output to JSON form.
   */
  @GET
  @Path("/system")
  @Produces({MediaType.TEXT_PLAIN})
  public Response system() {
    try {
      before();
      Runtime runtime = Runtime.getRuntime();
      OperatingSystemMXBean systemMxBean =
          (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
      RuntimeMXBean rb = ManagementFactory.getRuntimeMXBean();
      MemoryMXBean mb = ManagementFactory.getMemoryMXBean();
      MemoryUsage memNonHeap = mb.getNonHeapMemoryUsage();
      MemoryUsage memHeap = mb.getHeapMemoryUsage();

      StringBuilder sb = new StringBuilder();
      int availableCores = runtime.availableProcessors();
      double cpuLoad = systemMxBean.getSystemCpuLoad();
      double procLoad = systemMxBean.getProcessCpuLoad();

      sb.append("Server uptime (ms): ").append(rb.getUptime()).append("\n\n");

      sb.append("Available Processor Cores: ").append(availableCores).append("\n");
      sb.append("System CPU Load: ").append(cpuLoad).append("\n");
      sb.append("Process CPU Load: ").append(procLoad).append("\n\n");

      sb.append("Non-Heap Used Memory (KB): ").append(memNonHeap.getUsed() / 1024).append("\n");
      sb.append("Non-Heap Committed Memory (KB): ")
          .append((memNonHeap.getCommitted() / 1024))
          .append("\n");
      sb.append("Non-Heap Max Memory (KB): ").append((memNonHeap.getMax() / 1024)).append("\n\n");

      sb.append("Heap Used Memory (KB): ").append(memHeap.getUsed() / 1024).append("\n");
      sb.append("Heap Committed Memory (KB): ")
          .append((memHeap.getCommitted() / 1024))
          .append("\n");
      sb.append("Heap Max Memory (KB): ").append((memHeap.getMax() / 1024)).append("\n\n");

      sb.append("Max Memory (KB): ").append((runtime.maxMemory() / 1024)).append("\n");

      return Response.ok(sb.toString(), MediaType.TEXT_PLAIN).build();
    } catch (RuntimeException rtex) {
      return handleException(rtex);
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * INFO endpoint is meant to show the information about the NNA instance in PLAINTEXT form. TODO:
   * Convert the output to JSON form.
   */
  @GET
  @Path("/info")
  @Produces(MediaType.TEXT_PLAIN)
  public Response info() {
    final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
    @SuppressWarnings("unchecked")
    final List<BaseQuery> runningQueries =
        (List<BaseQuery>) context.getAttribute(NNA_RUNNING_QUERIES);
    final AtomicBoolean cancelRequest = (AtomicBoolean) context.getAttribute(NNA_CANCEL_REQUEST);
    try {
      before();

      StringBuilder sb = new StringBuilder();

      sb.append("Current active connections: ").append(runningQueries.size()).append("\n");
      sb.append("Current running queries:\n");
      for (BaseQuery query : runningQueries) {
        sb.append(query.toString()).append("\n");
      }
      sb.append("\n");

      boolean isInit = nnLoader.isInit();
      boolean isHistorical = nnLoader.isHistorical();
      boolean isProvidingSuggestions = nnLoader.getSuggestionsEngine().isLoaded();
      sb.append("Current system time (ms): ").append(Time.now()).append("\n");
      sb.append("Ready to service queries: ").append(isInit).append("\n");
      sb.append("Guarding against queries: ").append(cancelRequest.get()).append("\n");
      sb.append("Ready to service history: ").append(isHistorical).append("\n");
      sb.append("Ready to service suggestions: ").append(isProvidingSuggestions).append("\n\n");
      if (isInit) {
        long allSetSize = nnLoader.getINodeSet(INodeSet.all.name()).size();
        long fileSetSize = nnLoader.getINodeSet(INodeSet.files.name()).size();
        long dirSetSize = nnLoader.getINodeSet(INodeSet.dirs.name()).size();
        sb.append("Current TxID: ").append(nnLoader.getCurrentTxId()).append("\n");
        sb.append("Analysis TxID: ")
            .append(nnLoader.getSuggestionsEngine().getTransactionCount())
            .append("\n");
        sb.append("Analysis TxID delta: ")
            .append(nnLoader.getSuggestionsEngine().getTransactionCountDiff())
            .append("\n");
        sb.append("INode GSet size: ").append(allSetSize).append("\n\n");
        sb.append("INodeFile set size: ").append(fileSetSize).append("\n");
        sb.append("INodeFile set percentage: ")
            .append(((fileSetSize * 100.0f) / allSetSize))
            .append("\n\n");
        sb.append("INodeDir set size: ").append(dirSetSize).append("\n");
        sb.append("INodeDir set percentage: ")
            .append(((dirSetSize * 100.0f) / allSetSize))
            .append("\n\n");
      }
      sb.append("Cached directories for analysis::\n");
      Set<String> dirs = nnLoader.getSuggestionsEngine().getDirectoriesForAnalysis();
      sb.append("Cached directories size: ").append(dirs.size());
      for (String dir : dirs) {
        sb.append("\n").append(dir);
      }
      sb.append("\n\n");
      sb.append("Cached queries for analysis::\n");
      Map<String, String> queries = nnLoader.getSuggestionsEngine().getQueriesForAnalysis();
      sb.append("Cached queries size: ").append(queries.size()).append("\n");
      for (Entry<String, String> queryEntry : queries.entrySet()) {
        sb.append(queryEntry.getKey()).append(" : ").append(queryEntry.getValue()).append("\n");
      }
      return Response.ok(sb.toString(), MediaType.TEXT_PLAIN).build();
    } catch (RuntimeException rtex) {
      return handleException(rtex);
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * CONFIG endpoint is a dump of the system configuration in XML form. TODO: Convert the output to
   * JSON form.
   */
  @GET
  @Path("/config")
  @Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_XML})
  public Response config() {
    final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
    try {
      before();

      String key = request.getParameter("key");
      if (key != null && !key.isEmpty()) {
        return Response.ok(nnLoader.getConfigValue(key), MediaType.TEXT_PLAIN).build();
      } else {
        nnLoader.dumpConfig(response);
        return Response.ok().type(MediaType.APPLICATION_XML).build();
      }
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * REFRESH endpoint is an admin endpoint meant to change the set of read, write, and admin users.
   */
  @GET
  @Path("/refresh")
  @Produces({MediaType.TEXT_PLAIN})
  public Response refresh() {
    final SecurityContext securityContext =
        (SecurityContext) context.getAttribute(NNA_SECURITY_CONTEXT);
    try {
      before();
      securityContext.refresh(new ApplicationConfiguration());
      return Response.ok(securityContext.toString(), MediaType.TEXT_PLAIN).build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * LOG endpoint is an admin-level endpoint meant to dump the last "limit" bytes of the log file.
   */
  @GET
  @Path("/log")
  @Produces({MediaType.TEXT_PLAIN})
  public Response log() {
    final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
    try {
      before();
      Integer charsLimit = Integer.parseInt(request.getParameter("limit"));
      nnLoader.dumpLog(charsLimit, response);
      return Response.ok().build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /** DUMP endpoint is for dumping an INode path's information in PLAINTEXT form. */
  @GET
  @Path("/dump")
  @Produces({MediaType.APPLICATION_JSON})
  public Response dump() {
    final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
    try {
      before();
      if (!nnLoader.isInit()) {
        try (Writer writer = response.getWriter()) {
          writer.write("Namesystem is not fully initialized.\n");
        }
        return Response.ok().build();
      }
      String path = request.getParameter("path");
      nnLoader.dumpINodeInDetail(path, response);
      return Response.ok().build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /** SUGGESTIONS endpoint is an admin-level endpoint meant to dump the cached analysis by NNA. */
  @GET
  @Path("/suggestions")
  @Produces(MediaType.APPLICATION_JSON)
  public Response suggestions() {
    final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
    try {
      before();
      boolean allJson = request.getParameterMap().containsKey("all");
      if (allJson) {
        return Response.ok(
                nnLoader.getSuggestionsEngine().getAllSuggestionsAsJson(),
                MediaType.APPLICATION_JSON)
            .build();
      } else {
        String username = request.getParameter("username");
        return Response.ok(
                nnLoader.getSuggestionsEngine().getSuggestionsAsJson(username),
                MediaType.APPLICATION_JSON)
            .build();
      }
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * CACHEDMAPS endpoint is a cache-level endpoint meant to dump meta keys of cached analysis by
   * NNA.
   */
  @GET
  @Path("/cachedMaps")
  @Produces(MediaType.APPLICATION_JSON)
  public Response cachedMaps() {
    final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
    try {
      before();
      return Response.ok(
              Histograms.toJson(nnLoader.getSuggestionsEngine().getCachedMapKeys()),
              MediaType.APPLICATION_JSON)
          .build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * DIRECTORIES endpoint is an reader-level endpoint meant to dump the cached directory analysis by
   * NNA.
   */
  @GET
  @Path("/directories")
  @Produces(MediaType.APPLICATION_JSON)
  public Response directories() {
    try {
      final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
      before();
      String directory = request.getParameter("dir");
      String sum = request.getParameter("sum");
      if (sum == null || sum.isEmpty()) {
        sum = "count";
      }
      return Response.ok(
              nnLoader.getSuggestionsEngine().getDirectoriesAsJson(directory, sum),
              MediaType.APPLICATION_JSON)
          .build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * ADDDIRECTORY endpoint is an admin-level endpoint meant to add a directory for cached analysis
   * by NNA.
   */
  @GET
  @Path("/addDirectory")
  @Produces({MediaType.TEXT_PLAIN})
  public Response addDirectory() {
    try {
      final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);

      before();
      String directory = request.getParameter("dir");
      nnLoader.getSuggestionsEngine().addDirectoryToAnalysis(directory);
      return Response.ok(directory + " added for analysis.", MediaType.TEXT_PLAIN).build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * REMOVEDIRECTORY endpoint is an admin-level endpoint meant to remove a directory from analysis
   * by NNA.
   */
  @GET
  @Path("/removeDirectory")
  @Produces({MediaType.TEXT_PLAIN})
  public Response removeDirectory() {
    try {
      final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);

      before();
      String directory = request.getParameter("dir");
      nnLoader.getSuggestionsEngine().removeDirectoryFromAnalysis(directory);
      return Response.ok(directory + " removed from analysis.", MediaType.TEXT_PLAIN).build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * SETCACHEDQUERY endpoint is an admin-level endpoint meant to add a query for cached analysis by
   * NNA. It can also be used to modify existing cached queries by supplying an existing queryName.
   */
  @GET
  @Path("/setCachedQuery")
  @Produces({MediaType.TEXT_PLAIN})
  public Response setCachedQuery() {
    try {
      final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);

      before();
      String queryName = request.getParameter("queryName");
      String query = request.getQueryString();
      nnLoader.getSuggestionsEngine().setQueryToAnalysis(queryName, query);
      return Response.ok(queryName + " set for analysis.").build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * REMOVECACHEDQUERY eendpoint is an admin-level endpoint meant to remove a query from cached
   * analysis by NNA.
   */
  @GET
  @Path("/removeCachedQuery")
  @Produces({MediaType.TEXT_PLAIN})
  public Response removeCachedQuery() {
    try {
      final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);

      before();
      String queryName = request.getParameter("queryName");
      nnLoader.getSuggestionsEngine().removeQueryFromAnalysis(queryName);
      return Response.ok(queryName + " removed from analysis.").build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * GETCACHEDQUERY endpoint is an admin-level endpoint meant to add a query for cached analysis by
   * NNA. It can also be used to modify existing cached queries by supplying an existing queryName.
   */
  @GET
  @Path("/getCachedQuery")
  @Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
  public Response getCachedQuery() {
    try {
      final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);

      before();
      String queryName = request.getParameter("queryName");
      String body = nnLoader.getSuggestionsEngine().getLatestCacheQueryResult(queryName, response);
      return Response.ok(body).build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * FILEAGE endpoint is an reader-level endpoint meant to dump the cached file age analysis by NNA.
   */
  @GET
  @Path("/fileAge")
  @Produces(MediaType.APPLICATION_JSON)
  public Response fileAge() {
    try {
      final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);

      before();
      String sum = request.getParameter("sum");
      if (sum == null || sum.isEmpty()) {
        sum = "count";
      }
      return Response.ok(
              nnLoader.getSuggestionsEngine().getFileAgeAsJson(sum), MediaType.APPLICATION_JSON)
          .build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * QUOTAS endpoint is a reader-level endpoint meant to dump the cached set of detected quotas by
   * NNA.
   */
  @GET
  @Path("/fileTypes")
  @Produces(MediaType.APPLICATION_JSON)
  public Response fileTypes() {
    try {
      final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
      before();
      boolean allJson = request.getParameterMap().containsKey("all");
      String resp;
      if (allJson) {
        resp = nnLoader.getSuggestionsEngine().getAllFileTypeAsJson();
      } else {
        String user = request.getParameter("user");
        String sum = request.getParameter("sum");
        resp = nnLoader.getSuggestionsEngine().getFileTypeAsJson(user, sum);
      }
      return Response.ok(resp, MediaType.APPLICATION_JSON).build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * QUOTAS endpoint is a reader-level endpoint meant to dump the cached set of detected quotas by
   * NNA.
   */
  @GET
  @Path("/quotas")
  @Produces(MediaType.APPLICATION_JSON)
  public Response quotas() {
    try {
      final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
      before();
      boolean allJson = request.getParameterMap().containsKey("all");
      String sum = request.getParameter("sum");
      String resp;
      if (allJson) {
        resp = nnLoader.getSuggestionsEngine().getAllQuotasAsJson(sum);
      } else {
        String user = request.getParameter("user");
        resp = nnLoader.getSuggestionsEngine().getQuotaAsJson(user, sum);
      }
      return Response.ok(resp, MediaType.APPLICATION_JSON).build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * USERS endpoint is an admin-level endpoint meant to dump the cached set of detected users by
   * NNA.
   */
  @GET
  @Path("/users")
  @Produces(MediaType.APPLICATION_JSON)
  public Response users() {
    try {
      final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
      before();
      String suggestion = request.getParameter("suggestion");
      return Response.ok(
              nnLoader.getSuggestionsEngine().getUsersAsJson(suggestion),
              MediaType.APPLICATION_JSON)
          .build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /** TOP endpoint is an admin-level endpoint meant to dump the cached set of top issues by NNA. */
  @GET
  @Path("/top")
  @Produces(MediaType.APPLICATION_JSON)
  public Response top() {
    try {
      final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
      before();
      String strLimit = request.getParameter("limit");
      Integer limit;
      if (strLimit == null) {
        limit = 10;
      } else {
        limit = Integer.parseInt(strLimit);
      }
      return Response.ok(
              nnLoader.getSuggestionsEngine().getIssuesAsJson(limit, false),
              MediaType.APPLICATION_JSON)
          .build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * BOTTOM endpoint is an admin-level endpoint meant to dump the cached set of bottom issues by
   * NNA. The use-case here is for finding users to 'clear out'.
   */
  @GET
  @Path("/bottom")
  @Produces(MediaType.APPLICATION_JSON)
  public Response bottom() {
    try {
      final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
      before();
      String strLimit = request.getParameter("limit");
      Integer limit;
      if (strLimit == null) {
        limit = 10;
      } else {
        limit = Integer.parseInt(strLimit);
      }
      return Response.ok(
              nnLoader.getSuggestionsEngine().getIssuesAsJson(limit, true),
              MediaType.APPLICATION_JSON)
          .build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * TOKEN endpoint returns a set of user names and the last known DelegationToken issuance date.
   */
  @GET
  @Path("/token")
  @Produces(MediaType.APPLICATION_JSON)
  public Response token() {
    try {
      final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
      before();
      return Response.ok(nnLoader.getSuggestionsEngine().getTokens(), MediaType.APPLICATION_JSON)
          .build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * SAVENAMESPACE endpoint is an admin-level endpoint meant to dump the in-memory INode set to a
   * fresh FSImage.
   */
  @GET
  @Path("/saveNamespace")
  @Produces({MediaType.TEXT_PLAIN})
  public Response saveNamespace() {
    final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
    final AtomicBoolean savingNamespace =
        (AtomicBoolean) context.getAttribute(NNA_SAVING_NAMESPACE);
    try {
      before();
      String dirStr = request.getParameter("dir");
      String dir = (dirStr != null) ? dirStr : "/usr/local/nn-analytics/dfs/name/legacy/";
      String legacyBoolStr = request.getParameter("legacy");
      Boolean legacy = (legacyBoolStr == null) ? null : Boolean.parseBoolean(legacyBoolStr);
      if (savingNamespace.get()) {
        return Response.ok("Already saving namespace.", MediaType.TEXT_PLAIN).build();
      }
      savingNamespace.set(true);
      if (legacy != null && legacy) {
        nnLoader.saveLegacyNamespace(dir);
      } else {
        nnLoader.saveNamespace();
      }
      return Response.ok("Saving namespace.<br />Done.", MediaType.TEXT_PLAIN).build();
    } catch (Exception ex) {
      LOG.warn("Namespace save failed.", ex);
      return handleException(ex);
    } finally {
      after();
      savingNamespace.set(false);
    }
  }

  /**
   * FETCHNAMESPACE endpoint is an admin-level endpoint meant to fetch a fresh, up-to-date, FSImage
   * from the active cluster.
   */
  @GET
  @Path("/fetchNamespace")
  @Produces({MediaType.TEXT_PLAIN})
  public Response fetchNamespace() {
    final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
    StringWriter writer = new StringWriter();
    try {
      before();
      writer.write("Attempting to bootstrap namespace.<br />");
      try {
        TransferFsImageWrapper transferFsImage = new TransferFsImageWrapper(nnLoader);
        transferFsImage.downloadMostRecentImage();
        writer.write(
            "Done.<br />Please reload by going to `/reloadNamespace` "
                + "or by `service nn-analytics restart` on command line.");
      } catch (Throwable e) {
        writer.write("Bootstrap failed: " + e);
      }
      return Response.ok(writer.toString(), MediaType.TEXT_PLAIN).build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * RELOADNAMESPACE endpoint is an admin-level endpoint meant to clear the current in-memory INode
   * set and reload it from the latest FSImage found in the NNA instance's configured namespace
   * directory.
   */
  @GET
  @Path("/reloadNamespace")
  @Produces({MediaType.TEXT_PLAIN})
  public Response reloadNamespace() {
    final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
    final ReentrantReadWriteLock queryLock =
        (ReentrantReadWriteLock) context.getAttribute(NNA_QUERY_LOCK);
    final ApplicationConfiguration conf =
        (ApplicationConfiguration) context.getAttribute(NNA_APP_CONF);
    try {
      before();
      queryLock.writeLock().lock();
      StringWriter writer = new StringWriter();
      try {
        nnLoader.clear(true);
        nnLoader.load(null, null, conf);
        writer.write("Reload complete.");
      } catch (Throwable e) {
        writer.write("Reload failed: " + e);
      } finally {
        queryLock.writeLock().unlock();
      }
      return Response.ok(writer.toString(), MediaType.TEXT_PLAIN).build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /** HISTORY endpoint returns a set of data points from DB-stored suggestion snapshots. */
  @GET
  @Path("/history")
  @Produces({MediaType.APPLICATION_JSON})
  public Response history() {
    try {
      final HsqlDriver hsqlDriver = (HsqlDriver) context.getAttribute(NNA_HSQL_DRIVER);
      before();
      String usernameStr = request.getParameter("username");
      String username = (usernameStr == null) ? "" : usernameStr;
      String fromDate = request.getParameter("fromDate");
      String toDate = request.getParameter("toDate");
      return Response.ok(
              hsqlDriver.getAllHistory(fromDate, toDate, username), MediaType.APPLICATION_JSON)
          .build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /** DROP endpoint is an admin-level endpoint meant to drop and rebuild embedded DB tables. */
  @GET
  @Path("/drop")
  @Produces({MediaType.TEXT_PLAIN})
  public Response drop() {
    try {
      final HsqlDriver hsqlDriver = (HsqlDriver) context.getAttribute(NNA_HSQL_DRIVER);
      before();
      String table = request.getParameter("table");
      hsqlDriver.rebuildTable(table);
      return Response.ok("Successfully re-built table: " + table, MediaType.TEXT_PLAIN).build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * TRUNCATE endpoint is an admin-level endpoint meant to truncate the history of embedded DB
   * tables.
   */
  @GET
  @Path("/truncate")
  @Produces({MediaType.TEXT_PLAIN})
  public Response truncate() {
    try {
      final HsqlDriver hsqlDriver = (HsqlDriver) context.getAttribute(NNA_HSQL_DRIVER);
      before();
      String table = request.getParameter("table");
      Integer limit = Integer.parseInt(request.getParameter("limit"));
      hsqlDriver.truncateTable(table, limit);

      return Response.ok(
              "Successfully truncated table: " + table + ", to last: " + limit + " days.",
              MediaType.TEXT_PLAIN)
          .build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * DIVIDE endpoint takes 2 sets of "set", "filter", "sum" parameters and returns the result of the
   * filtered set "1" divided by filtered set "2" in PLAINTEXT form.
   */
  @GET
  @Path("/divide")
  @Produces({MediaType.TEXT_PLAIN})
  public Response divide() {
    final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
    final ReentrantReadWriteLock queryLock =
        (ReentrantReadWriteLock) context.getAttribute(NNA_QUERY_LOCK);
    final AtomicBoolean cancelRequest = (AtomicBoolean) context.getAttribute(NNA_CANCEL_REQUEST);
    try {
      before();

      if (!nnLoader.isInit()) {
        PrintWriter writer = response.getWriter();
        writer.write("");
        writer.flush();
        writer.close();
        return Response.ok().build();
      }

      queryLock.writeLock().lock();
      try {
        if (cancelRequest.get()) {
          throw new IOException("Query cancelled.");
        }
        String filterStr1 = request.getParameter("filters1");
        String filterStr2 = request.getParameter("filters2");
        String emailsToStr = request.getParameter("emailTo");
        String emailsCcStr = request.getParameter("emailCC");
        String emailFrom = request.getParameter("emailFrom");
        String emailHost = request.getParameter("emailHost");
        String emailConditionsStr = request.getParameter("emailConditions");
        String[] filters1 = Helper.parseFilters(filterStr1);
        String[] filterOps1 = Helper.parseFilterOps(filterStr1);
        String[] filters2 = Helper.parseFilters(filterStr2);
        String[] filterOps2 = Helper.parseFilterOps(filterStr2);
        String[] emailsTo = (emailsToStr != null) ? emailsToStr.split(",") : null;
        String[] emailsCc = (emailsCcStr != null) ? emailsCcStr.split(",") : null;
        String set1 = request.getParameter("set1");
        String set2 = request.getParameter("set2");
        String sumStr1 = request.getParameter("sum1");
        String sum1 = (sumStr1 != null) ? sumStr1 : "count";
        String sumStr2 = request.getParameter("sum2");
        String sum2 = (sumStr2 != null) ? sumStr2 : "count";
        QueryChecker.isValidQuery(set1, filters1, null, sum1, filterOps1, null);
        QueryChecker.isValidQuery(set2, filters2, null, sum2, filterOps2, null);

        Collection<INode> inodes1 = Helper.performFilters(nnLoader, set1, filters1, filterOps1);
        Collection<INode> inodes2 = Helper.performFilters(nnLoader, set2, filters2, filterOps2);

        if (!sum1.isEmpty() && !sum2.isEmpty()) {
          long sumValue1 = nnLoader.getQueryEngine().sum(inodes1, sum1);
          long sumValue2 = nnLoader.getQueryEngine().sum(inodes2, sum2);
          float division = (float) sumValue1 / (float) sumValue2;

          LOG.info("The result of " + sumValue1 + " dividied by " + sumValue2 + " is: " + division);
          String message;
          if (division > 100) {
            message = String.valueOf(((long) division));
          } else {
            message = String.valueOf(division);
          }
          if (emailsTo != null && emailsTo.length != 0 && emailHost != null && emailFrom != null) {
            String subject = nnLoader.getAuthority() + " | DIVISION";
            try {
              if (emailConditionsStr != null) {
                MailOutput.check(emailConditionsStr, (long) division, nnLoader);
              }
              MailOutput.write(subject, message, emailHost, emailsTo, emailsCc, emailFrom);
            } catch (Exception e) {
              LOG.info("Failed to email output with exception: ", e);
            }
          }
          PrintWriter writer = response.getWriter();
          writer.write(message);
          writer.flush();
          writer.close();
        }
      } finally {
        queryLock.writeLock().unlock();
      }
      return Response.ok().build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * Filter endpoint takes 1 set of "set", "filter", "sum" / "limit" parameters and returns either
   * the list of file paths that pass the filters or the summation of the INode fields that pass the
   * filters in PLAINTEXT form. TODO: Consider separating logic of "list of file paths" to /dump
   * endpoint. TODO: Move "&filterOps=" into API of "&filters=" by making filter triplets separated
   * by ":".
   */
  @GET
  @Path("/filter")
  @Produces({MediaType.TEXT_PLAIN})
  public Response filter() {
    final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
    final ReentrantReadWriteLock queryLock =
        (ReentrantReadWriteLock) context.getAttribute(NNA_QUERY_LOCK);
    final AtomicBoolean cancelRequest = (AtomicBoolean) context.getAttribute(NNA_CANCEL_REQUEST);
    try {
      before();

      if (!nnLoader.isInit()) {
        PrintWriter writer = response.getWriter();
        writer.write("");
        writer.flush();
        writer.close();
        return Response.ok().build();
      }

      queryLock.writeLock().lock();
      try {
        if (cancelRequest.get()) {
          throw new IOException("Query cancelled.");
        }
        String fullFilterStr = request.getParameter("filters");
        String emailsToStr = request.getParameter("emailTo");
        String emailsCcStr = request.getParameter("emailCC");
        String emailFrom = request.getParameter("emailFrom");
        String emailHost = request.getParameter("emailHost");
        String find = request.getParameter("find");
        String emailConditionsStr = request.getParameter("emailConditions");
        String[] filters = Helper.parseFilters(fullFilterStr);
        String[] filterOps = Helper.parseFilterOps(fullFilterStr);
        String[] emailsTo = (emailsToStr != null) ? emailsToStr.split(",") : null;
        String[] emailsCc = (emailsCcStr != null) ? emailsCcStr.split(",") : null;
        String set = request.getParameter("set");
        String sumStr = request.getParameter("sum");
        String[] sums = (sumStr != null) ? sumStr.split(",") : new String[] {"count"};
        String limitStr = request.getParameter("limit");
        Integer limit;
        if (limitStr == null) {
          limit = Integer.MAX_VALUE;
        } else {
          limit = Integer.parseInt(limitStr);
        }

        for (String sum : sums) {
          QueryChecker.isValidQuery(set, filters, null, sum, filterOps, find);
        }

        Collection<INode> filteredINodes =
            Helper.performFilters(nnLoader, set, filters, filterOps, find);

        if (sums.length == 1 && sumStr != null) {
          String sum = sums[0];
          long sumValue = nnLoader.getQueryEngine().sum(filteredINodes, sum);
          String message = String.valueOf(sumValue);
          if (emailsTo != null && emailsTo.length != 0 && emailHost != null && emailFrom != null) {
            String subject =
                nnLoader.getAuthority()
                    + " | "
                    + sum
                    + " | "
                    + set
                    + " | Filters: "
                    + fullFilterStr;
            try {
              if (emailConditionsStr != null) {
                MailOutput.check(emailConditionsStr, sumValue, nnLoader);
              }
              MailOutput.write(subject, message, emailHost, emailsTo, emailsCc, emailFrom);
            } catch (Exception e) {
              LOG.info("Failed to email output with exception:", e);
            }
          }
          LOG.info("Returning filter result: " + message);
          PrintWriter writer = response.getWriter();
          writer.write(message);
          writer.flush();
          writer.close();
        } else if (sums.length > 1 && sumStr != null) {
          StringBuilder message = new StringBuilder();
          for (String sum : sums) {
            long sumValue = nnLoader.getQueryEngine().sum(filteredINodes, sum);
            message.append(sumValue).append("\n");
          }
          PrintWriter writer = response.getWriter();
          writer.write(message.toString());
          writer.flush();
          writer.close();
        } else {
          nnLoader.getQueryEngine().dumpINodePaths(filteredINodes, limit, response);
        }
      } finally {
        queryLock.writeLock().unlock();
      }
      return Response.ok().build();
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * Histogram endpoint takes 1 set of "set", "filter", "type", and "sum" parameters and returns a
   * histogram where the X-axis represents the "type" type and the Y-axis represents the "sum" type.
   * Output types available dictated by "&histogramOutput=". Default is CHART form. TODO: Consider
   * separating logic of "list of file paths" to /dump endpoint. TODO: Move "&filterOps=" into API
   * of "&filters=" by making filter triplets separated by ":". TODO: Consider renaming "type"
   * parameter to something more meaningful.
   */
  @GET
  @Path("/histogram")
  @Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
  public Response histogram() {
    final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
    final ReentrantReadWriteLock queryLock =
        (ReentrantReadWriteLock) context.getAttribute(NNA_QUERY_LOCK);
    final AtomicBoolean cancelRequest = (AtomicBoolean) context.getAttribute(NNA_CANCEL_REQUEST);
    try {
      before();

      if (!nnLoader.isInit()) {
        response.setHeader("Content-Type", "application/json");
        String message = Histograms.toChartJsJson(new HashMap<>(), "not_loaded", "", "");
        PrintWriter writer = response.getWriter();
        writer.write(message);
        writer.flush();
        writer.close();
        return Response.ok().build();
      }

      queryLock.writeLock().lock();
      try {
        if (cancelRequest.get()) {
          throw new IOException("Query cancelled.");
        }
        final String fullFilterStr = request.getParameter("filters");
        final String histogramConditionsStr = request.getParameter("histogramConditions");
        final String emailsToStr = request.getParameter("emailTo");
        final String emailsCcStr = request.getParameter("emailCC");
        final String emailFrom = request.getParameter("emailFrom");
        final String emailHost = request.getParameter("emailHost");
        final String emailConditionsStr = request.getParameter("emailConditions");
        final String[] filters = Helper.parseFilters(fullFilterStr);
        final String[] filterOps = Helper.parseFilterOps(fullFilterStr);
        final String histType = request.getParameter("type");
        final String set = request.getParameter("set");
        final String topStr = request.getParameter("top");
        final Integer top = (topStr == null) ? null : Integer.parseInt(topStr);
        final String bottomStr = request.getParameter("bottom");
        final Integer bottom = (bottomStr == null) ? null : Integer.parseInt(bottomStr);
        final String sumStr = request.getParameter("sum");
        final Boolean useLock = Boolean.parseBoolean(request.getParameter("useLock"));
        final String sortAscendingStr = request.getParameter("sortAscending");
        final Boolean sortAscending =
            sortAscendingStr == null ? null : Boolean.parseBoolean(sortAscendingStr);
        final String sortDescendingStr = request.getParameter("sortDescending");
        final Boolean sortDescending =
            sortDescendingStr == null ? null : Boolean.parseBoolean(sortDescendingStr);
        final String sum = (sumStr != null) ? sumStr : "count";
        final String[] emailsTo = (emailsToStr != null) ? emailsToStr.split(",") : null;
        final String[] emailsCc = (emailsCcStr != null) ? emailsCcStr.split(",") : null;
        final String transformConditionsStr = request.getParameter("transformConditions");
        final String transformFieldsStr = request.getParameter("transformFields");
        final String transformOutputsStr = request.getParameter("transformOutputs");
        final String parentDirDepthStr = request.getParameter("parentDirDepth");
        final Integer parentDirDepth =
            (parentDirDepthStr == null) ? null : Integer.parseInt(parentDirDepthStr);
        final String timeRangeStr = request.getParameter("timeRange");
        final String timeRange = (timeRangeStr != null) ? timeRangeStr : "weekly";
        final String outputTypeStr = request.getParameter("histogramOutput");
        final String outputType = (outputTypeStr != null) ? outputTypeStr : "chart";
        final String type = request.getParameter("type");
        final String find = request.getParameter("find");
        final String rawTimestampsStr = request.getParameter("rawTimestamps");
        final boolean rawTimestamps = Boolean.parseBoolean(rawTimestampsStr);

        QueryChecker.isValidQuery(set, filters, type, sum, filterOps, find);
        Stream<INode> filteredINodes = Helper.setFilters(nnLoader, set, filters, filterOps);

        Map<String, Function<INode, Long>> transformMap =
            Transforms.getAttributeTransforms(
                transformConditionsStr, transformFieldsStr, transformOutputsStr, nnLoader);
        Map<String, Long> histogram;
        final long startTime = System.currentTimeMillis();
        String binLabels;

        nnLoader.namesystemWriteLock(useLock);
        try {
          HistogramInvoker histogramInvoker =
              new HistogramInvoker(
                      nnLoader.getQueryEngine(),
                      histType,
                      sum,
                      parentDirDepth,
                      timeRange,
                      find,
                      filteredINodes,
                      transformMap,
                      histogramConditionsStr,
                      top,
                      bottom,
                      sortAscending,
                      sortDescending)
                  .invoke();
          histogram = histogramInvoker.getHistogram();
          binLabels = histogramInvoker.getBinLabels();
        } finally {
          nnLoader.namesystemWriteUnlock(useLock);
        }

        // Perform conditions filtering.
        if (histogramConditionsStr != null && !histogramConditionsStr.isEmpty()) {
          histogram =
              nnLoader.getQueryEngine().removeKeysOnConditional(histogram, histogramConditionsStr);
        }

        // Slice top and bottom.
        if (top != null && bottom != null) {
          throw new IllegalArgumentException("Please choose only one type of slice.");
        } else if (top != null && top > 0) {
          histogram = Histograms.sliceToTop(histogram, top);
        } else if (bottom != null && bottom > 0) {
          histogram = Histograms.sliceToBottom(histogram, bottom);
        }

        // Sort results.
        if (sortAscending != null && sortDescending != null) {
          throw new IllegalArgumentException("Please choose one type of sort.");
        } else if (sortAscending != null && sortAscending) {
          histogram = Histograms.sortByValue(histogram, true);
        } else if (sortDescending != null && sortDescending) {
          histogram = Histograms.sortByValue(histogram, false);
        }

        long endTime = System.currentTimeMillis();
        LOG.info("Performing histogram: " + histType + " took: " + (endTime - startTime) + " ms.");

        // Email out.
        if (emailsTo != null && emailsTo.length != 0 && emailHost != null && emailFrom != null) {
          String subject =
              nnLoader.getAuthority()
                  + " | X: "
                  + histType
                  + " | Y: "
                  + sum
                  + " | "
                  + set
                  + " | Filters: "
                  + fullFilterStr;
          try {
            Set<String> highlightKeys = new HashSet<>();
            if (emailConditionsStr != null) {
              MailOutput.check(emailConditionsStr, histogram, highlightKeys, nnLoader);
            }
            MailOutput.write(
                subject, histogram, highlightKeys, emailHost, emailsTo, emailsCc, emailFrom);
            return Response.ok().build();
          } catch (Exception e) {
            LOG.info("Failed to email output with exception:", e);
          }
        }

        // Return final histogram to Web UI as output type.
        HistogramOutput output = HistogramOutput.valueOf(outputType);
        PrintWriter writer;
        String message;
        switch (output) {
          case chart:
            response.setHeader("Content-Type", "application/json");
            message =
                Histograms.toChartJsJson(
                    histogram, Helper.toTitle(histType, sum), Helper.toYAxis(sum), binLabels);
            writer = response.getWriter();
            writer.write(message);
            writer.flush();
            writer.close();
            return Response.ok().type(MediaType.APPLICATION_JSON).build();
          case json:
            response.setHeader("Content-Type", "application/json");
            message = Histograms.toJson(histogram);
            writer = response.getWriter();
            writer.write(message);
            writer.flush();
            writer.close();
            return Response.ok().type(MediaType.APPLICATION_JSON).build();
          case csv:
            response.setHeader("Content-Type", "text/plain");
            message = Histograms.toCsv(histogram, find, rawTimestamps);
            writer = response.getWriter();
            writer.write(message);
            writer.flush();
            writer.close();
            return Response.ok().type(MediaType.APPLICATION_JSON).build();
          default:
            throw new IllegalArgumentException(
                "Could not determine output type: "
                    + histType
                    + ".\nPlease check /histogramOutputs for available histogram outputs.");
        }
      } finally {
        queryLock.writeLock().unlock();
      }
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * HISTOGRAM2 endpoint takes 1 set of "set", "filter", "type", and "sum" parameters and returns a
   * histogram where the X-axis represents the "type" type and the Y-axis represents the "sum" type.
   * Output types available dictated by "&histogramOutput=". Default is CHART form. This differs
   * from Histogram endpoint in that it can group by multiple fields in a single query.
   */
  @GET
  @Path("/histogram2")
  @Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
  public Response histogram2() {
    final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
    final ReentrantReadWriteLock queryLock =
        (ReentrantReadWriteLock) context.getAttribute(NNA_QUERY_LOCK);
    final AtomicBoolean cancelRequest = (AtomicBoolean) context.getAttribute(NNA_CANCEL_REQUEST);
    try {
      before();

      if (!nnLoader.isInit()) {
        response.setHeader("Content-Type", "application/json");
        String message = Histograms.toChartJsJson(new HashMap<>(), "not_loaded", "", "");
        PrintWriter writer = response.getWriter();
        writer.write(message);
        writer.flush();
        writer.close();
        return Response.ok().build();
      }

      queryLock.writeLock().lock();
      try {
        if (cancelRequest.get()) {
          throw new IOException("Query cancelled.");
        }
        final String fullFilterStr = request.getParameter("filters");
        final String[] filters = Helper.parseFilters(fullFilterStr);
        final String[] filterOps = Helper.parseFilterOps(fullFilterStr);
        final String set = request.getParameter("set");
        final String sum = request.getParameter("sum");
        final Boolean useLock = Boolean.parseBoolean(request.getParameter("useLock"));
        final String parentDirDepthStr = request.getParameter("parentDirDepth");
        final Integer parentDirDepth =
            (parentDirDepthStr == null) ? null : Integer.parseInt(parentDirDepthStr);
        final String outputTypeStr = request.getParameter("histogramOutput");
        final String timeRangeStr = request.getParameter("timeRange");
        final String timeRange = (timeRangeStr != null) ? timeRangeStr : "weekly";
        final String outputType = (outputTypeStr != null) ? outputTypeStr : "json";
        final String typeStr = request.getParameter("type");
        final String[] types = (typeStr != null) ? typeStr.split(",") : new String[0];

        for (String type : types) {
          QueryChecker.isValidQuery(set, filters, type, sum, filterOps, null);
        }

        Map<String, Map<String, Long>> histogram;
        final long startTime = System.currentTimeMillis();
        nnLoader.namesystemWriteLock(useLock);
        try {
          HistogramTwoLevelInvoker histogramInvoker =
              new HistogramTwoLevelInvoker(
                      nnLoader.getQueryEngine(),
                      types[0],
                      types[1],
                      sum,
                      parentDirDepth,
                      timeRange,
                      nnLoader.getINodeSet(set).parallelStream())
                  .invoke();
          histogram = histogramInvoker.getHistogram();
        } finally {
          nnLoader.namesystemWriteUnlock(useLock);
        }
        long endTime = System.currentTimeMillis();
        LOG.info("Performing histogram2: " + typeStr + " took: " + (endTime - startTime) + " ms.");

        // Return final histogram to Web UI as output type.
        HistogramOutput output = HistogramOutput.valueOf(outputType);
        String message;
        PrintWriter writer;
        switch (output) {
          case json:
            response.setHeader("Content-Type", "application/json");
            writer = response.getWriter();
            message = Histograms.toJson(histogram);
            writer.write(message);
            writer.flush();
            writer.close();
            return Response.ok().build();
          case csv:
            response.setHeader("Content-Type", "text/plain");
            writer = response.getWriter();
            message = Histograms.twoLeveltoCsv(histogram);
            writer.write(message);
            writer.flush();
            writer.close();
            return Response.ok().build();
          default:
            throw new IllegalArgumentException(
                "Could not determine output type: "
                    + outputType
                    + ".\nPlease check /histogramOutputs for available histogram outputs.");
        }
      } finally {
        queryLock.writeLock().unlock();
      }
    } catch (RuntimeException rtex) {
      return handleException(rtex);
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * HISTOGRAM3 endpoint takes 1 set of "set", "filter", "type", and "sum" parameters and returns a
   * histogram where the X-axis represents the "type" type and the Y-axis represents the "sum" type.
   * Output types available dictated by "&histogramOutput=". Default is CHART form. This differs
   * from Histogram endpoint in that it can output multiple sums and values in a single query.
   */
  @GET
  @Path("/histogram3")
  @Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
  public Response histogram3() {
    final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
    final ReentrantReadWriteLock queryLock =
        (ReentrantReadWriteLock) context.getAttribute(NNA_QUERY_LOCK);
    final AtomicBoolean cancelRequest = (AtomicBoolean) context.getAttribute(NNA_CANCEL_REQUEST);
    try {
      before();

      if (!nnLoader.isInit()) {
        response.setHeader("Content-Type", "application/json");
        String message = Histograms.toChartJsJson(new HashMap<>(), "not_loaded", "", "");
        PrintWriter writer = response.getWriter();
        writer.write(message);
        writer.flush();
        writer.close();
        return Response.ok().build();
      }

      queryLock.writeLock().lock();
      try {
        if (cancelRequest.get()) {
          throw new IOException("Query cancelled.");
        }
        final String fullFilterStr = request.getParameter("filters");
        final String[] filters = Helper.parseFilters(fullFilterStr);
        final String[] filterOps = Helper.parseFilterOps(fullFilterStr);
        final String histType = request.getParameter("type");
        final String set = request.getParameter("set");
        final String sumStr = request.getParameter("sum");
        final String sortAscendingIndexStr = request.getParameter("sortAscendingIndex");
        final Integer sortAscendingIndex =
            (sortAscendingIndexStr == null) ? null : Integer.parseInt(sortAscendingIndexStr);
        final String sortDescendingIndexStr = request.getParameter("sortDescendingIndex");
        final Integer sortDescendingIndex =
            (sortDescendingIndexStr == null) ? null : Integer.parseInt(sortDescendingIndexStr);
        final String histogramConditionsStr = request.getParameter("histogramConditions");
        final Boolean useLock = Boolean.parseBoolean(request.getParameter("useLock"));
        final String[] sums = (sumStr != null) ? sumStr.split(",") : new String[0];
        final String parentDirDepthStr = request.getParameter("parentDirDepth");
        final Integer parentDirDepth =
            (parentDirDepthStr == null) ? null : Integer.parseInt(parentDirDepthStr);
        final String outputTypeStr = request.getParameter("histogramOutput");
        final String timeRangeStr = request.getParameter("timeRange");
        final String timeRange = (timeRangeStr != null) ? timeRangeStr : "weekly";
        final String outputType = (outputTypeStr != null) ? outputTypeStr : "json";
        final String type = request.getParameter("type");
        final String findStr = request.getParameter("find");
        final String[] finds = (findStr != null) ? findStr.split(",") : new String[0];

        for (String sum : sums) {
          QueryChecker.isValidQuery(set, filters, type, sum, filterOps, null);
        }
        for (String find : finds) {
          QueryChecker.isValidQuery(set, filters, type, null, filterOps, find);
        }
        Collection<INode> filteredINodes = Helper.performFilters(nnLoader, set, filters, filterOps);
        List<Map<String, Long>> histograms = new ArrayList<>(sums.length + finds.length);

        final long startTime = System.currentTimeMillis();
        for (int i = 0, j = 0; i < sums.length || j < finds.length; ) {
          Map<String, Long> histogram;
          String sum = null;
          String find = null;
          if (i < sums.length) {
            sum = sums[i];
            i++;
          } else {
            find = finds[j];
            j++;
          }

          nnLoader.namesystemWriteLock(useLock);
          try {
            HistogramInvoker histogramInvoker =
                new HistogramInvoker(
                        nnLoader.getQueryEngine(),
                        histType,
                        sum,
                        parentDirDepth,
                        timeRange,
                        find,
                        filteredINodes.parallelStream(),
                        null,
                        histogramConditionsStr,
                        null,
                        null,
                        null,
                        null)
                    .invoke();
            histogram = histogramInvoker.getHistogram();
          } finally {
            nnLoader.namesystemWriteUnlock(useLock);
          }
          histograms.add(histogram);
        }

        Map<String, List<Long>> mergedHistogram =
            histograms
                .parallelStream()
                .flatMap(m -> m.entrySet().stream())
                .collect(
                    Collectors.groupingBy(
                        Entry::getKey,
                        Collector.of(
                            ArrayList<Long>::new,
                            (list, item) -> list.add(item.getValue()),
                            (left, right) -> {
                              left.addAll(right);
                              return left;
                            })));

        // Perform conditions filtering.
        if (histogramConditionsStr != null && !histogramConditionsStr.isEmpty()) {
          mergedHistogram =
              nnLoader
                  .getQueryEngine()
                  .removeKeysOnConditional2(mergedHistogram, histogramConditionsStr);
        }

        // Sort results.
        if (sortAscendingIndex != null && sortDescendingIndex != null) {
          throw new IllegalArgumentException("Please choose one type of sort index.");
        } else if (sortAscendingIndex != null) {
          mergedHistogram = Histograms.sortByValue(mergedHistogram, sortAscendingIndex, true);
        } else if (sortDescendingIndex != null) {
          mergedHistogram = Histograms.sortByValue(mergedHistogram, sortDescendingIndex, false);
        }

        long endTime = System.currentTimeMillis();
        LOG.info("Performing histogram3: " + histType + " took: " + (endTime - startTime) + " ms.");

        // Return final histogram to Web UI as output type.
        HistogramOutput output = HistogramOutput.valueOf(outputType);
        String message;
        PrintWriter writer;
        switch (output) {
          case json:
            response.setHeader("Content-Type", "application/json");
            writer = response.getWriter();
            message = Histograms.toJson(mergedHistogram);
            writer.write(message);
            writer.flush();
            writer.close();
            return Response.ok().build();
          case csv:
            response.setHeader("Content-Type", "text/plain");
            writer = response.getWriter();
            message = Histograms.toCsv(mergedHistogram);
            writer.write(message);
            writer.flush();
            writer.close();
            return Response.ok().build();
          default:
            throw new IllegalArgumentException(
                "Could not determine output type: "
                    + histType
                    + ".\nPlease check /histogramOutputs for available histogram outputs.");
        }
      } finally {
        queryLock.writeLock().unlock();
      }
    } catch (RuntimeException rtex) {
      return handleException(rtex);
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * HISTOGRAM3 endpoint takes 1 set of "set", "filter", "type", and "sum" parameters and returns a
   * histogram where the X-axis represents the "type" type and the Y-axis represents the "sum" type.
   * Output types available dictated by "&histogramOutput=". Default is CHART form. This differs
   * from Histogram endpoint in that it can output multiple sums and values in a single query.
   */
  @GET
  @Path("/contentSummary")
  @Produces({MediaType.TEXT_PLAIN, MediaType.APPLICATION_JSON})
  public Response contentSummary() {
    final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
    final ReentrantReadWriteLock queryLock =
        (ReentrantReadWriteLock) context.getAttribute(NNA_QUERY_LOCK);
    final AtomicBoolean cancelRequest = (AtomicBoolean) context.getAttribute(NNA_CANCEL_REQUEST);
    try {
      before();

      if (!nnLoader.isInit()) {
        PrintWriter writer = response.getWriter();
        writer.write("");
        writer.flush();
        writer.close();
        return Response.ok().build();
      }

      String path = request.getParameter("path");
      final Boolean useFsLock = Boolean.parseBoolean(request.getParameter("useLock"));
      final Boolean useQueryLock = Boolean.parseBoolean(request.getParameter("useQueryLock"));

      ContentSummary contentSummary;

      if (useQueryLock != null && useQueryLock) {
        queryLock.writeLock().lock();
      }
      if (cancelRequest.get()) {
        throw new IOException("Query cancelled.");
      }
      nnLoader.namesystemWriteLock(useFsLock);
      try {
        contentSummary = nnLoader.getContentSummaryImpl(path);
      } finally {
        nnLoader.namesystemWriteUnlock(useFsLock);
        if (useQueryLock != null && useQueryLock) {
          queryLock.writeLock().unlock();
        }
      }
      return Response.ok(contentSummary.toString(), MediaType.TEXT_PLAIN).build();
    } catch (RuntimeException rtex) {
      return handleException(rtex);
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * SUBMITOPERATION is a GET only call that only WRITER users can access. It take parameters like
   * /filter and schedules operations to perform.
   */
  @GET
  @Path("/submitOperation")
  @Produces({MediaType.TEXT_PLAIN})
  public Response submitOperation() {
    final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
    final SecurityContext securityContext =
        (SecurityContext) context.getAttribute(NNA_SECURITY_CONTEXT);
    final ReentrantReadWriteLock queryLock =
        (ReentrantReadWriteLock) context.getAttribute(NNA_QUERY_LOCK);
    final ApplicationConfiguration conf =
        (ApplicationConfiguration) context.getAttribute(NNA_APP_CONF);
    @SuppressWarnings("unchecked")
    final Map<String, BaseOperation> runningOperations =
        (Map<String, BaseOperation>) context.getAttribute(NNA_RUNNING_OPERATIONS);
    final ExecutorService operationService =
        (ExecutorService) context.getAttribute(NNA_OPERATION_SERVICE);
    try {

      if (!nnLoader.isInit()) {
        response.setHeader("Content-Type", "application/json");
        String message = "Not accepting operations yet.";
        PrintWriter writer = response.getWriter();
        writer.write(message);
        writer.flush();
        writer.close();
        return Response.ok().build();
      }

      queryLock.writeLock().lock();
      try {
        final String fullFilterStr = request.getParameter("filters");
        final String find = request.getParameter("find");
        final String[] filters = Helper.parseFilters(fullFilterStr);
        final String[] filterOps = Helper.parseFilterOps(fullFilterStr);
        final String set = request.getParameter("set");
        final String operation = request.getParameter("operation");
        String sleepStr = request.getParameter("sleep");
        Integer sleep;
        if (sleepStr == null) {
          sleep = Constants.DEFAULT_DELETE_SLEEP_MS;
        } else {
          sleep = Integer.parseInt(sleepStr);
        }

        if (operation == null || operation.isEmpty()) {
          throw new IllegalArgumentException("No operation defined. Please check /operations.");
        }
        QueryChecker.isValidQuery(set, filters, null, null, filterOps, find);

        Collection<INode> filteredINodes =
            Helper.performFilters(nnLoader, set, filters, filterOps, find);
        if (filteredINodes.size() == 0) {
          LOG.info("Skipping operation request because it resulted in empty INode set.");
          throw new IOException(
              "Skipping operation request because it resulted in empty INode set.");
        }

        FileSystem fs = nnLoader.getFileSystem();
        String[] operationSplits = operation.split(":");
        String logBaseDir = conf.getBaseDir();
        BaseOperation operationObj;
        switch (operationSplits[0]) {
          case "delete":
            operationObj =
                new Delete(
                    filteredINodes,
                    request.getQueryString(),
                    securityContext.getUserName(),
                    logBaseDir,
                    fs);
            break;
          case "setReplication":
            short newReplFactor = Short.parseShort(operationSplits[1]);
            operationObj =
                new SetReplication(
                    filteredINodes,
                    request.getQueryString(),
                    securityContext.getUserName(),
                    logBaseDir,
                    fs,
                    newReplFactor);
            break;
          case "setStoragePolicy":
            operationObj =
                new SetStoragePolicy(
                    filteredINodes,
                    request.getQueryString(),
                    securityContext.getUserName(),
                    logBaseDir,
                    fs,
                    operationSplits[1]);
            break;
          default:
            throw new IllegalArgumentException(
                "Unknown operation:" + operationSplits[0] + ". Please check /operations.");
        }
        runningOperations.put(operationObj.identity(), operationObj);

        final int finalSleep = sleep;
        Callable<Boolean> callable =
            () -> {
              try {
                operationObj.initialize();
                while (operationObj.hasNext()) {
                  try {
                    if (finalSleep >= 100) {
                      Thread.sleep(finalSleep);
                    }
                  } catch (InterruptedException ignored) {
                    LOG.debug("Operation sleep interrupted due to: ", ignored);
                  }
                  operationObj.performOp();
                }
                operationObj.close();
                return Boolean.TRUE;
              } catch (IllegalStateException e) {
                operationObj.abort();
                LOG.info("Aborted operation due to: ", e);
                return Boolean.FALSE;
              } finally {
                runningOperations.remove(operationObj.identity());
              }
            };
        Future<Boolean> submittedOperation = operationService.submit(callable);
        operationObj.linkFuture(submittedOperation);

        PrintWriter writer = response.getWriter();
        writer.write(operationObj.identity());
        writer.flush();
        writer.close();
        return Response.ok().build();
      } finally {
        queryLock.writeLock().unlock();
      }
    } catch (RuntimeException rtex) {
      return handleException(rtex);
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * LISTOPERATIONS is a GET only call that only WRITER users can access. It takes an optional
   * `?identity=STRING&limit=INT` argument to see a specific operation.
   */
  @GET
  @Path("/listOperations")
  @Produces({MediaType.TEXT_PLAIN})
  public Response listOperations() {
    try {
      final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
      final ReentrantReadWriteLock queryLock =
          (ReentrantReadWriteLock) context.getAttribute(NNA_QUERY_LOCK);
      @SuppressWarnings("unchecked")
      final Map<String, BaseOperation> runningOperations =
          (Map<String, BaseOperation>) context.getAttribute(NNA_RUNNING_OPERATIONS);

      before();

      if (!nnLoader.isInit()) {
        response.setHeader("Content-Type", "application/json");
        PrintWriter writer = response.getWriter();
        writer.write("");
        writer.flush();
        writer.close();
        return Response.ok().build();
      }

      queryLock.writeLock().lock();
      try {
        String identity = request.getParameter("identity");
        String limitStr = request.getParameter("limit");
        Integer limit;
        if (limitStr == null) {
          limit = 1;
        } else {
          limit = Integer.parseInt(limitStr);
        }

        PrintWriter writer;
        if (identity == null || identity.length() == 0) {
          Set<Entry<String, BaseOperation>> entries = runningOperations.entrySet();
          StringBuilder sb = new StringBuilder();
          sb.append("Total Operations: ").append(entries.size()).append('\n');
          for (Entry<String, BaseOperation> entry : entries) {
            BaseOperation operation = entry.getValue();
            sb.append("ID: ");
            sb.append(entry.getKey());
            sb.append(", total: ");
            sb.append(operation.totalToPerform());
            sb.append(", operated: ");
            sb.append(operation.numPerformed());
            sb.append(" || owner: ");
            sb.append(operation.owner());
            sb.append(" || type: ");
            sb.append(operation.type());
            sb.append(" || query: ");
            sb.append(operation.query());
            sb.append('\n');
          }
          writer = response.getWriter();
          writer.write(sb.toString());
          writer.flush();
          writer.close();
        } else {
          BaseOperation operation = runningOperations.get(identity);
          if (operation == null) {
            throw new FileNotFoundException("Operation not found.");
          }
          StringBuilder sb = new StringBuilder();
          sb.append("Identity: ");
          sb.append(operation.identity());
          sb.append('\n');
          sb.append("Owner: ");
          sb.append(operation.owner());
          sb.append('\n');
          sb.append("Type: ");
          sb.append(operation.type());
          sb.append('\n');
          sb.append("Query: ");
          sb.append(operation.query());
          sb.append('\n');
          sb.append("Total to perform: ");
          int totalToPerform = operation.totalToPerform();
          sb.append(totalToPerform);
          sb.append('\n');
          sb.append("Total performed: ");
          int numPerformed = operation.numPerformed();
          sb.append(numPerformed);
          sb.append('\n');
          sb.append("Total left to perform: ");
          sb.append(totalToPerform - numPerformed);
          sb.append('\n');
          sb.append("Percentage done: ");
          double percentageDone = ((double) numPerformed / (double) totalToPerform) * 100.0;
          sb.append(percentageDone);
          sb.append('\n');
          sb.append("Up next: ");
          sb.append(operation.upNext());
          sb.append('\n');
          sb.append("Last (");
          sb.append(limit);
          sb.append(") performed: ");
          List<String> lastDeleted = operation.lastPerformed(limit);
          Collections.reverse(lastDeleted);
          sb.append(lastDeleted.toString());
          sb.append('\n');
          writer = response.getWriter();
          writer.write(sb.toString());
          writer.flush();
          writer.close();
        }
      } finally {
        queryLock.writeLock().unlock();
      }
      return Response.ok().build();
    } catch (RuntimeException rtex) {
      return handleException(rtex);
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * ABORTOPERTATION is a GET only call that only WRITER users can access. It takes a required
   * parameter `?identity=STRING` argument to specify which operation to stop.
   */
  @GET
  @Path("/abortOperation")
  @Produces({MediaType.TEXT_PLAIN})
  public Response abortOperation() {
    try {
      final NameNodeLoader nnLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
      @SuppressWarnings("unchecked")
      final Map<String, BaseOperation> runningOperations =
          (Map<String, BaseOperation>) context.getAttribute(NNA_RUNNING_OPERATIONS);
      final ReentrantReadWriteLock queryLock =
          (ReentrantReadWriteLock) context.getAttribute(NNA_QUERY_LOCK);

      before();

      if (!nnLoader.isInit()) {
        response.setHeader("Content-Type", "application/json");
        PrintWriter writer = response.getWriter();
        writer.write("");
        writer.flush();
        writer.close();
        return Response.ok().build();
      }

      queryLock.writeLock().lock();
      try {
        String identity = request.getParameter("identity");
        String limitStr = request.getParameter("limit");
        Integer limit;
        if (limitStr == null) {
          limit = 1;
        } else {
          limit = Integer.parseInt(limitStr);
        }
        BaseOperation operation = runningOperations.get(identity);
        if (operation == null) {
          throw new FileNotFoundException("Operation not found.");
        }
        operation.abort();

        LOG.info("Aborted Operation: " + operation.identity());

        int totalToPerform = operation.totalToPerform();
        int numPerformed = operation.numPerformed();
        double percentageDone = ((double) numPerformed / (double) totalToPerform) * 100.0;
        List<String> lastPerformed = operation.lastPerformed(limit);
        Collections.reverse(lastPerformed);

        String sb =
            "Identity: "
                + operation.identity()
                + '\n'
                + "Owner: "
                + operation.owner()
                + '\n'
                + "Query: "
                + operation.query()
                + '\n'
                + "Total to perform: "
                + totalToPerform
                + '\n'
                + "Total performed: "
                + numPerformed
                + '\n'
                + "Total left to perform: "
                + (totalToPerform - numPerformed)
                + '\n'
                + "Percentage done: "
                + percentageDone
                + '\n'
                + "Up next: "
                + operation.upNext()
                + '\n'
                + "Last ("
                + limit
                + ") performed: "
                + lastPerformed.toString()
                + "\n\n";
        PrintWriter writer = response.getWriter();
        writer.write(sb);
        writer.flush();
        writer.close();
      } finally {
        queryLock.writeLock().unlock();
      }
      return Response.ok().build();
    } catch (RuntimeException rtex) {
      return handleException(rtex);
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  /**
   * SQL is a POST only call that only READER users can access. It takes a form document as an
   * argument that specifies an SQL query to perform on NNA.
   */
  @POST
  @Path("/sql")
  public Response sql(MultivaluedMap<String, String> formData) {
    try {
      final NameNodeLoader nameNodeLoader = (NameNodeLoader) context.getAttribute(NNA_NN_LOADER);
      before();

      if (!nameNodeLoader.isInit()) {
        return Response.ok()
            .entity(new StringEntity("not_loaded"))
            .type(MediaType.TEXT_PLAIN_TYPE)
            .build();
      }

      String sqlStatement = request.getParameter("sqlStatement");
      if (Strings.isNullOrEmpty(sqlStatement)) {
        sqlStatement = formData.getFirst("sqlStatement");
      }

      SqlParser sqlParser = new SqlParser();

      if (sqlStatement.toUpperCase().contains("SHOW TABLES")) {
        String jsonShowTables = sqlParser.showTables();
        return Response.ok(jsonShowTables, MediaType.APPLICATION_JSON).build();
      }
      if (sqlStatement.contains("DESCRIBE")) {
        String jsonDescription = sqlParser.describeInJson(sqlStatement);
        return Response.ok(jsonDescription, MediaType.APPLICATION_JSON).build();
      }

      sqlParser.parse(sqlStatement);
      boolean isHistogram = !Strings.isNullOrEmpty(sqlParser.getType());
      boolean hasSumOrFind =
          !Strings.isNullOrEmpty(sqlParser.getSum()) || !Strings.isNullOrEmpty(sqlParser.getFind());

      String set = sqlParser.getINodeSet();
      String[] filters = Helper.parseFilters(sqlParser.getFilters());
      String[] filterOps = Helper.parseFilterOps(sqlParser.getFilters());
      String type = sqlParser.getType();
      String sum = sqlParser.getSum();
      String find = sqlParser.getFind();
      Integer limit = sqlParser.getLimit();
      int parentDirDepth = sqlParser.getParentDirDepth();
      String timeRange = sqlParser.getTimeRange();
      Boolean sortAscending = sqlParser.getSortAscending();
      Boolean sortDescending = sqlParser.getSortDescending();
      Integer top = (limit != null && sortDescending != null && sortDescending) ? limit : null;
      Integer bottom = (limit != null && sortAscending != null && sortAscending) ? limit : null;

      if (isHistogram) {
        QueryChecker.isValidQuery(set, filters, type, sum, filterOps, find);
        Stream<INode> filteredINodes = Helper.setFilters(nameNodeLoader, set, filters, filterOps);
        HistogramInvoker histogramInvoker =
            new HistogramInvoker(
                    nameNodeLoader.getQueryEngine(),
                    type,
                    sum,
                    parentDirDepth,
                    timeRange,
                    find,
                    filteredINodes,
                    null,
                    null,
                    top,
                    bottom,
                    sortAscending,
                    sortDescending)
                .invoke();
        Map<String, Long> histogram = histogramInvoker.getHistogram();
        String histogramJson = Histograms.toJson(histogram);
        return Response.ok(histogramJson, MediaType.APPLICATION_JSON).build();
      } else {
        Collection<INode> filteredINodes =
            Helper.performFilters(nameNodeLoader, set, filters, filterOps, find);
        if (hasSumOrFind) {
          long sumValue = nameNodeLoader.getQueryEngine().sum(filteredINodes, sum);
          return Response.ok(sumValue, MediaType.TEXT_PLAIN).build();
        } else {
          nameNodeLoader.getQueryEngine().dumpINodePaths(filteredINodes, limit, response);
          return Response.ok().type(MediaType.TEXT_PLAIN).build();
        }
      }
    } catch (RuntimeException rtex) {
      return handleException(rtex);
    } catch (Exception ex) {
      return handleException(ex);
    } finally {
      after();
    }
  }

  private Response handleException(Exception ex) {
    final SecurityContext securityContext =
        (SecurityContext) context.getAttribute(NNA_SECURITY_CONTEXT);
    @SuppressWarnings("unchecked")
    final List<BaseQuery> runningQueries =
        (List<BaseQuery>) context.getAttribute(NNA_RUNNING_QUERIES);
    LOG.info("EXCEPTION encountered: ", ex);
    LOG.info(Arrays.toString(ex.getStackTrace()));
    try {
      if (ex instanceof AuthenticationException || ex instanceof BadCredentialsException) {
        return Response.status(HttpStatus.SC_UNAUTHORIZED)
            .entity(ex.toString())
            .type(MediaType.TEXT_PLAIN)
            .build();
      } else if (ex instanceof AuthorizationException) {
        return Response.status(HttpStatus.SC_FORBIDDEN)
            .entity(ex.toString())
            .type(MediaType.TEXT_PLAIN)
            .build();
      } else if (ex instanceof MalformedURLException || ex instanceof SQLException) {
        return Response.status(HttpStatus.SC_BAD_REQUEST)
            .entity(ex.toString())
            .type(MediaType.TEXT_PLAIN)
            .build();
      } else if (ex instanceof FileNotFoundException) {
        return Response.status(HttpStatus.SC_NOT_FOUND)
            .entity(ex.getMessage())
            .type(MediaType.TEXT_PLAIN)
            .build();
      } else {
        StringBuilder sb = new StringBuilder();
        sb.append("Query failed! Stacktrace is below:\n");
        sb.append(
            "You can check all available queries at /sets, /filters, /filterOps, /histograms, and /sums.\n\n");
        StackTraceElement[] stackTrace = ex.getStackTrace();
        sb.append(ex.getLocalizedMessage());
        for (StackTraceElement element : stackTrace) {
          sb.append(element.toString());
          sb.append("\n");
        }
        return Response.status(HttpStatus.SC_INTERNAL_SERVER_ERROR)
            .entity(sb.toString())
            .type(MediaType.TEXT_PLAIN)
            .build();
      }
    } finally {
      runningQueries.remove(Helper.createQuery(request, securityContext.getUserName()));
    }
  }

  private void before() throws AuthenticationException, HttpAction, AuthorizationException {
    final SecurityContext securityContext =
        (SecurityContext) context.getAttribute(NNA_SECURITY_CONTEXT);
    final UsageMetrics usageMetrics = (UsageMetrics) context.getAttribute(NNA_USAGE_METRICS);
    @SuppressWarnings("unchecked")
    final List<BaseQuery> runningQueries =
        (List<BaseQuery>) context.getAttribute(NNA_RUNNING_QUERIES);
    securityContext.handleAuthentication(request, response);
    securityContext.handleAuthorization(request, response);
    if (!securityContext.isLoginAttempt(request)) {
      runningQueries.add(Helper.createQuery(request, securityContext.getUserName()));
      usageMetrics.userMadeQuery(securityContext, request);
    }
  }

  private void after() {
    final SecurityContext securityContext =
        (SecurityContext) context.getAttribute(NNA_SECURITY_CONTEXT);
    @SuppressWarnings("unchecked")
    final List<BaseQuery> runningQueries =
        (List<BaseQuery>) context.getAttribute(NNA_RUNNING_QUERIES);
    if (!securityContext.isLoginAttempt(request)) {
      runningQueries.remove(Helper.createQuery(request, securityContext.getUserName()));
    }
  }
}
