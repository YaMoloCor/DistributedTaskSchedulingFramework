package org.example.job.admin.thread;

import org.example.job.admin.conf.JobAdminConfig;
import org.example.job.admin.model.JobInfo;
import org.example.job.admin.model.JobLog;
import org.example.job.admin.trigger.TriggerTypeEnum;
import org.example.job.admin.util.I18nUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class JobFailMonitorHelper {
    private static Logger logger = LoggerFactory.getLogger(JobFailMonitorHelper.class);

    private static JobFailMonitorHelper instance = new JobFailMonitorHelper();

    public static JobFailMonitorHelper getInstance() {
        return instance;
    }

    private Thread monitorThread;
    private volatile boolean toStop = false;

    public void start() {
        monitorThread = new Thread(() -> {
            while (!toStop) {
                try {
                    List<Long> failLogIds = JobAdminConfig.getAdminConfig().getJobLogDao().findFailJobLogIds(1000);//告警状态==0 返回代码!=200
                    if (failLogIds != null && !failLogIds.isEmpty()) {
                        for (long failLogId : failLogIds) {
                            // lock log
                            int lockRet = JobAdminConfig.getAdminConfig().getJobLogDao().updateAlarmStatus(failLogId, 0, -1);
                            if (lockRet < 1) {
                                continue;
                            }

                            JobLog jobLog = JobAdminConfig.getAdminConfig().getJobLogDao().load(failLogId);
                            JobInfo jobInfo = JobAdminConfig.getAdminConfig().getJobInfoDao().loadById(jobLog.getJobId());
                            if (jobLog.getExecutorFailRetryCount() > 0) {
                                JobTriggerPoolHelper.trigger(jobLog.getJobId(),
                                        TriggerTypeEnum.RETRY,
                                        (jobLog.getExecutorFailRetryCount() - 1),
                                        jobLog.getExecutorShardingParam(),
                                        jobLog.getExecutorParam(),
                                        null);
                                String retryMsg = "<br><br><span style=\"color:#F39C12;\" > >>>>>>>>>>>" + I18nUtil.getString("jobconf_trigger_type_retry") + "<<<<<<<<<<< </span><br>";
                                jobLog.setTriggerMsg(jobLog.getTriggerMsg() + retryMsg);
                                JobAdminConfig.getAdminConfig().getJobLogDao().updateTriggerInfo(jobLog);
                            }

                            // 2、fail alarm monitor
                            int newAlarmStatus = 0;        // 告警状态：0-默认、-1=锁定状态、1-无需告警、2-告警成功、3-告警失败
                            if (jobInfo != null && jobInfo.getAlarmEmail() != null && jobInfo.getAlarmEmail().trim().length() > 0) {
                                boolean alarmResult = true; //TODO JobAdminConfig.getAdminConfig().getJobAlarmer().alarm(jobInfo, jobLog);
                                newAlarmStatus = alarmResult ? 2 : 3;
                            } else {
                                newAlarmStatus = 1;
                            }

                            JobAdminConfig.getAdminConfig().getJobLogDao().updateAlarmStatus(failLogId, -1, newAlarmStatus);

                        }
                    }
                } catch (Exception e) {
                    if (!toStop) {
                        logger.error(">>>>>>>>>>> xxl-job, job fail monitor thread error:{}", e);
                    }
                }
                try {
                    TimeUnit.SECONDS.sleep(10);
                } catch (Exception e) {
                    if (!toStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }

            logger.info(">>>>>>>>>>> job, job fail monitor thread stop");
        });
        monitorThread.setDaemon(true);
        monitorThread.setName("job, admin JobFailMonitorHelper");
        monitorThread.start();
    }

    public void stop() {
        toStop = true;
        // interrupt and wait
        monitorThread.interrupt();
        try {
            monitorThread.join();
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }
}
