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

package azkaban.utils;

import azkaban.Constants;
import azkaban.Constants.ConfigurationKeys;
import azkaban.alert.Alerter;
import azkaban.executor.ExecutableFlow;
import azkaban.executor.Executor;
import azkaban.executor.ExecutorLoader;
import azkaban.executor.ExecutorManagerException;
import azkaban.executor.mail.DefaultMailCreator;
import azkaban.executor.mail.MailCreator;
import azkaban.metrics.CommonMetrics;
import azkaban.sla.SlaOption;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimaps;
import org.apache.commons.collections.CollectionUtils;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.log4j.Logger;

import javax.inject.Inject;
import javax.inject.Singleton;
import javax.mail.internet.AddressException;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@Singleton
public class Emailer extends AbstractMailer implements Alerter {

    private static final String HTTPS = "https";
    private static final String HTTP = "http";
    private static final Logger logger = Logger.getLogger(Emailer.class);
    private final CommonMetrics commonMetrics;
    private final String scheme;
    private final String clientHostname;
    private final String clientPortNumber;
    private final String azkabanName;
    private final ExecutorLoader executorLoader;

    @Inject
    public Emailer(final Props props, final CommonMetrics commonMetrics,
                   final EmailMessageCreator messageCreator, final ExecutorLoader executorLoader) {
        super(props, messageCreator);
        this.executorLoader = requireNonNull(executorLoader, "executorLoader is null.");
        this.commonMetrics = requireNonNull(commonMetrics, "commonMetrics is null.");
        this.azkabanName = props.getString("azkaban.name", "azkaban");

        final int mailTimeout = props.getInt("mail.timeout.millis", 30000);
        EmailMessage.setTimeout(mailTimeout);
        final int connectionTimeout = props.getInt("mail.connection.timeout.millis", 30000);
        EmailMessage.setConnectionTimeout(connectionTimeout);

        EmailMessage.setTotalAttachmentMaxSize(getAttachmentMaxSize());

        this.clientHostname = props.getString(ConfigurationKeys.AZKABAN_WEBSERVER_EXTERNAL_HOSTNAME,
                props.getString("jetty.hostname", "localhost"));

        if (props.getBoolean("jetty.use.ssl", true)) {
            this.scheme = HTTPS;
            this.clientPortNumber = Integer.toString(props
                    .getInt(ConfigurationKeys.AZKABAN_WEBSERVER_EXTERNAL_SSL_PORT,
                            props.getInt("jetty.ssl.port",
                                    Constants.DEFAULT_SSL_PORT_NUMBER)));
        } else {
            this.scheme = HTTP;
            this.clientPortNumber = Integer.toString(
                    props.getInt(ConfigurationKeys.AZKABAN_WEBSERVER_EXTERNAL_PORT, props.getInt("jetty.port",
                            Constants.DEFAULT_PORT_NUMBER)));
        }
    }

    public String getAzkabanURL() {
        return this.scheme + "://" + this.clientHostname + ":" + this.clientPortNumber;
    }

    /**
     * Send an email to the specified email list
     */
    public void sendEmail(final List<String> emailList, final String subject, final String body) {
        if (emailList != null && !emailList.isEmpty()) {
            final EmailMessage message = super.createEmailMessage(subject, "text/html", emailList);
            message.setBody(body);
            sendEmail(message, true, "email message " + body);
        }
    }

    @Override
    public void alertOnSla(final SlaOption slaOption, final String slaMessage) {
        final String subject =
                "SLA violation for " + getJobOrFlowName(slaOption) + " on " + getAzkabanName();
        final List<String> emailList =
                (List<String>) slaOption.getEmails();
        logger.info("Sending SLA email " + slaMessage);
        sendEmail(emailList, subject, slaMessage);
    }

    @Override
    public void alertOnFirstError(final ExecutableFlow flow) {
        final EmailMessage message = this.messageCreator.createMessage();
        final MailCreator mailCreator = getMailCreator(flow);
        final boolean mailCreated = mailCreator.createFirstErrorMessage(flow, message, this.azkabanName,
                this.scheme, this.clientHostname, this.clientPortNumber);
        sendEmail(message, mailCreated,
                "first error email message for execution " + flow.getExecutionId());
    }

    @Override
    public void alertOnError(final ExecutableFlow flow, final String... extraReasons) {
        final EmailMessage message = this.messageCreator.createMessage();
        final MailCreator mailCreator = getMailCreator(flow);
        List<ExecutableFlow> last72hoursExecutions = new ArrayList<>();

        if (flow.getStartTime() > 0) {
            final long startTime = flow.getStartTime() - Duration.ofHours(72).toMillis();
            try {
                last72hoursExecutions = this.executorLoader.fetchFlowHistory(flow.getProjectId(), flow
                        .getFlowId(), startTime);
            } catch (final ExecutorManagerException e) {
                logger.error("unable to fetch past executions", e);
            }
        }

        final boolean mailCreated = mailCreator.createErrorEmail(flow, last72hoursExecutions, message,
                this.azkabanName, this.scheme, this.clientHostname, this.clientPortNumber, extraReasons);
        //发送信息
        String status = "error";
        isSendTextMessage(this.azkabanName, flow, last72hoursExecutions, status);
        sendEmail(message, mailCreated, "error email message for execution " + flow.getExecutionId());
    }

    /**
     * 是否发送信息
     *
     * @param azkabanName
     * @param flow
     * @param last72hoursExecutions
     * @param status
     * @return
     */
    private boolean isSendTextMessage(String azkabanName, ExecutableFlow flow, List<ExecutableFlow> last72hoursExecutions, String status) {
        //工作流名称
        String flowId = flow.getFlowId();
        //执行id
        int executionId = flow.getExecutionId();
        //开始时间
        long startTime = flow.getStartTime();
        //结束时间
        long endTime = flow.getEndTime();
        return sendMessage(azkabanName, flowId, executionId, startTime, endTime, status);
    }

    /**
     * 发送请求
     *
     * @param azkabanName
     * @param flowId
     * @param executionId
     * @param startTime
     * @param endTime
     * @param status
     * @return
     */
    private boolean sendMessage(String azkabanName, String flowId, int executionId, long startTime, long endTime, String status) {
        //TODO 环境切换后续优化
        String baseUrl;
        String uatUrl = "http://mkl-datacenter.uat1.rs.com/data-c";
        String stgUrl = "http://mkl-datacenter.redstarclouds.cn/data-c";
        String prdUrl = "https://mkl-datacenter.redstarclouds.com/data-c";
        baseUrl = stgUrl;
        String url = baseUrl + "/api/conf/monitorRule/isSendTextMessage?azkabanName="
                + azkabanName + "&flowId=" + flowId + "&executionId=" + executionId +
                "&startTime=" + startTime + "&status=" + status + "&endTime=" + endTime;
        //发送get请求
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpGet get = new HttpGet(url);
        CloseableHttpResponse response = null;
        try {
            response = httpClient.execute(get);
            if (response != null && response.getStatusLine().getStatusCode() == 200) {
                logger.info("发送成功了");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                httpClient.close();
                if (response != null) {
                    response.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return false;
    }

    @Override
    public void alertOnSuccess(final ExecutableFlow flow) {
        final EmailMessage message = this.messageCreator.createMessage();
        final MailCreator mailCreator = getMailCreator(flow);
        final boolean mailCreated = mailCreator.createSuccessEmail(flow, message, this.azkabanName,
                this.scheme, this.clientHostname, this.clientPortNumber);
        //发送信息
        String status = "success";
        isSendTextMessage(this.azkabanName, flow, null, status);
        sendEmail(message, mailCreated, "success email message for execution " + flow.getExecutionId());
    }

    /**
     * Sends as many emails as there are unique combinations of:
     * <p>
     * [mail creator] x [failure email address list]
     * <p>
     * Executions with the same combo are grouped into a single message.
     */
    @Override
    public void alertOnFailedUpdate(final Executor executor, List<ExecutableFlow> flows,
                                    final ExecutorManagerException updateException) {

        flows = flows.stream()
                .filter(flow -> flow.getExecutionOptions() != null)
                .filter(flow -> CollectionUtils.isNotEmpty(flow.getExecutionOptions().getFailureEmails()))
                .collect(Collectors.toList());

        // group by mail creator in case some flows use different creators
        final ImmutableListMultimap<String, ExecutableFlow> creatorsToFlows = Multimaps
                .index(flows, flow -> flow.getExecutionOptions().getMailCreator());

        for (final String mailCreatorName : creatorsToFlows.keySet()) {

            final ImmutableList<ExecutableFlow> creatorFlows = creatorsToFlows.get(mailCreatorName);
            final MailCreator mailCreator = getMailCreator(mailCreatorName);

            // group by recipients in case some flows have different failure email addresses
            final ImmutableListMultimap<List<String>, ExecutableFlow> emailsToFlows = Multimaps
                    .index(creatorFlows, flow -> flow.getExecutionOptions().getFailureEmails());

            for (final List<String> emailList : emailsToFlows.keySet()) {
                sendFailedUpdateEmail(executor, updateException, mailCreator, emailsToFlows.get(emailList));
            }
        }
    }

    /**
     * Sends a single email about failed updates.
     */
    private void sendFailedUpdateEmail(final Executor executor,
                                       final ExecutorManagerException exception, final MailCreator mailCreator,
                                       final ImmutableList<ExecutableFlow> flows) {
        final EmailMessage message = this.messageCreator.createMessage();
        final boolean mailCreated = mailCreator
                .createFailedUpdateMessage(flows, executor, exception, message,
                        this.azkabanName, this.scheme, this.clientHostname, this.clientPortNumber);
        final List<Integer> executionIds = Lists.transform(flows, ExecutableFlow::getExecutionId);
        sendEmail(message, mailCreated, "failed update email message for executions " + executionIds);
    }

    private MailCreator getMailCreator(final ExecutableFlow flow) {
        final String name = flow.getExecutionOptions().getMailCreator();
        return getMailCreator(name);
    }

    private MailCreator getMailCreator(final String name) {
        final MailCreator mailCreator = DefaultMailCreator.getCreator(name);
        logger.debug("ExecutorMailer using mail creator:" + mailCreator.getClass().getCanonicalName());
        return mailCreator;
    }

    public void sendEmail(final EmailMessage message, final boolean mailCreated,
                          final String operation) {
        if (mailCreated) {
            try {
                message.sendEmail();
                logger.info("Sent " + operation);
                this.commonMetrics.markSendEmailSuccess();
            } catch (final Exception e) {
                logger.error("Failed to send " + operation, e);
                if (!(e instanceof AddressException)) {
                    this.commonMetrics.markSendEmailFail();
                }
            }
        }
    }

    private String getJobOrFlowName(final SlaOption slaOption) {
        if (org.apache.commons.lang.StringUtils.isNotBlank(slaOption.getJobName())) {
            return slaOption.getFlowName() + ":" + slaOption.getJobName();
        } else {
            return slaOption.getFlowName();
        }
    }
}
