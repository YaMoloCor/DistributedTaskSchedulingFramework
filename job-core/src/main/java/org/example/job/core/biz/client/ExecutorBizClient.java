package org.example.job.core.biz.client;

import org.example.job.core.biz.ExecutorBiz;
import org.example.job.core.biz.model.*;
import org.example.job.core.route.JobRemotingUtil;

/**
 * 远程调用
 */
public class ExecutorBizClient implements ExecutorBiz {

    private String addressUrl;

    private String accessToken;

    private int timeout = 3;

    public ExecutorBizClient() {
    }

    public ExecutorBizClient(String addressUrl, String accessToken) {
        this.addressUrl = addressUrl;
        this.accessToken = accessToken;

        // valid
        if (!this.addressUrl.endsWith("/")) {
            this.addressUrl = this.addressUrl + "/";
        }
    }

    @Override
    public ReturnT<String> run(TriggerParam triggerParam) {
        return JobRemotingUtil.postBody(addressUrl + "run", accessToken, timeout, triggerParam, String.class);
    }

    @Override
    public ReturnT<LogResult> log(LogParam logParam) {
        return JobRemotingUtil.postBody(addressUrl + "log", accessToken, timeout, logParam, LogResult.class);
    }

    @Override
    public ReturnT<String> kill(KillParam killParam) {
        return JobRemotingUtil.postBody(addressUrl + "kill", accessToken, timeout, killParam, String.class);
    }
}
