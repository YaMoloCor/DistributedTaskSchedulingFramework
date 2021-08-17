package org.example.job.admin.scheduler;

import org.example.job.admin.conf.JobAdminConfig;
import org.example.job.admin.thread.*;
import org.example.job.admin.util.I18nUtil;
import org.example.job.core.biz.ExecutorBiz;
import org.example.job.core.biz.client.ExecutorBizClient;
import org.example.job.core.enums.ExecutorBlockStrategyEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class JobScheduler {
    private static final Logger logger = LoggerFactory.getLogger(JobScheduler.class);

    public void init() {
        //TODO 初始化什么了？？？？ init i18n 单机串行 丢弃后续调度 覆盖之前调度
        initI18n();

        // 初始化trigger线程池
        JobTriggerPoolHelper.getInstance().start();

        //  执行器注册线程池和程序启动时刷新执行器（自动注册）
        JobRegistryHelper.getInstance().start();

        // 失败重试 报警提示 admin fail-monitor run
        JobFailMonitorHelper.getInstance().start();

        // callback线程池处理执行器完成回调 admin lose-monitor run ( depend on JobTriggerPoolHelper )
        JobCompleteHelper.getInstance().start();

        // admin log report start
        JobLogReportHelper.getInstance().start();

        // start-schedule  ( depend on JobTriggerPoolHelper )
        JobScheduleHelper.getInstance().start();

        logger.info(">>>>>>>>> init job admin success.");
    }

    public void destroy() throws Exception {
        // stop-schedule
        JobScheduleHelper.getInstance().stop();

        // admin log report stop
        JobLogReportHelper.getInstance().stop();

        // admin lose-monitor stop
        JobCompleteHelper.getInstance().stop();

        // admin fail-monitor stop
        JobFailMonitorHelper.getInstance().stop();

        // admin registry stop
        JobRegistryHelper.getInstance().stop();

        // admin trigger pool stop
        JobTriggerPoolHelper.getInstance().stop();
    }

    // ---------------------- I18n ----------------------

    /**
     * 单机串行 丢弃后续调度 覆盖之前调度
     */
    private void initI18n() {
        for (ExecutorBlockStrategyEnum item : ExecutorBlockStrategyEnum.values()) {
            item.setTitle(I18nUtil.getString("jobconf_block_".concat(item.name())));
        }
    }

    // ---------------------- executor-client ----------------------
    private static ConcurrentHashMap<String, ExecutorBiz> executorBizRepository = new ConcurrentHashMap<>();

    public static ExecutorBiz getExecutorBizClient(String address) throws Exception {
        //valid
        if (address == null || address.trim().length() == 0) {
            return null;
        }
        //load-cache
        address = address.trim();
        ExecutorBiz client = executorBizRepository.get(address);
        if (client != null) {
            return client;
        }
        // set-cache
        client = new ExecutorBizClient(address, JobAdminConfig.getAdminConfig().getAccessToken());
        executorBizRepository.put(address, client);
        return client;
    }
}
