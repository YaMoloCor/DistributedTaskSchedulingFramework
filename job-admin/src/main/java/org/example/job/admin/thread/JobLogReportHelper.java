package org.example.job.admin.thread;

import org.example.job.admin.conf.JobAdminConfig;
import org.example.job.admin.model.JobLogReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class JobLogReportHelper {
    private static Logger logger = LoggerFactory.getLogger(JobScheduleHelper.class);

    private static JobLogReportHelper instance = new JobLogReportHelper();

    public static JobLogReportHelper getInstance() {
        return instance;
    }

    private Thread logrThread;
    private volatile boolean toStop = false;

    public void start() {
        logrThread = new Thread(() -> {
            // last clean log time
            long lastCleanLogTime = 0;
            while (!toStop) {
                try {
                    //1更新3天内的成功失败总数
                    for (int i = 0; i < 3; i++) {
                        // today 一天前的00:00:00 到 23:59:59
                        Calendar itemDay = Calendar.getInstance();
                        itemDay.add(Calendar.DAY_OF_MONTH, -i);
                        itemDay.set(Calendar.HOUR_OF_DAY, 0);
                        itemDay.set(Calendar.MINUTE, 0);
                        itemDay.set(Calendar.SECOND, 0);
                        itemDay.set(Calendar.MILLISECOND, 0);
                        Date todayFrom = itemDay.getTime();

                        itemDay.set(Calendar.HOUR_OF_DAY, 23);
                        itemDay.set(Calendar.MINUTE, 59);
                        itemDay.set(Calendar.SECOND, 59);
                        itemDay.set(Calendar.MILLISECOND, 999);
                        Date todayTo = itemDay.getTime();

                        JobLogReport jobLogReport = new JobLogReport();
                        jobLogReport.setTriggerDay(todayFrom);
                        jobLogReport.setRunningCount(0);
                        jobLogReport.setSucCount(0);
                        jobLogReport.setFailCount(0);
                        Map<String, Object> triggerCountMap = JobAdminConfig.getAdminConfig().getJobLogDao().findLogReport(todayFrom, todayTo);
                        if (triggerCountMap != null && !triggerCountMap.isEmpty()) {
                            int triggerDayCount = triggerCountMap.containsKey("triggerDayCount") ? Integer.parseInt(String.valueOf(triggerCountMap.get("triggerDayCount"))) : 0;
                            int triggerDayCountRunning = triggerCountMap.containsKey("triggerDayCountRunning") ? Integer.parseInt(String.valueOf(triggerCountMap.get("triggerDayCountRunning"))) : 0;
                            int triggerDayCountSuc = triggerCountMap.containsKey("triggerDayCountSuc") ? Integer.parseInt(String.valueOf(triggerCountMap.get("triggerDayCountSuc"))) : 0;
                            int triggerDayCountFail = triggerDayCount - triggerDayCountRunning - triggerDayCountSuc;

                            jobLogReport.setRunningCount(triggerDayCountRunning);
                            jobLogReport.setSucCount(triggerDayCountSuc);
                            jobLogReport.setFailCount(triggerDayCountFail);
                        }
                        //update
                        int ret = JobAdminConfig.getAdminConfig().getJobLogReportDao().update(jobLogReport);
                        if (ret < 1) {
                            JobAdminConfig.getAdminConfig().getJobLogReportDao().save(jobLogReport);
                        }
                    }
                } catch (Exception e) {
                    if (!toStop) {
                        logger.error(">>>>>>>>>>> job, job log report thread error:{}", e);
                    }
                }
                //2清除日志 getLogretentiondays日志保留天数 小于七天为-1
                if (JobAdminConfig.getAdminConfig().getLogretentiondays() > 0
                        && System.currentTimeMillis() - lastCleanLogTime > 24*60*60*1000) {
                    Calendar expiredDay = Calendar.getInstance();
                    expiredDay.add(Calendar.DAY_OF_MONTH, -1 * JobAdminConfig.getAdminConfig().getLogretentiondays());
                    expiredDay.set(Calendar.HOUR_OF_DAY, 0);
                    expiredDay.set(Calendar.MINUTE, 0);
                    expiredDay.set(Calendar.SECOND, 0);
                    expiredDay.set(Calendar.MILLISECOND, 0);
                    Date clearBeforeTime = expiredDay.getTime();

                    List<Long> logIds = null;
                    do {
                        logIds = JobAdminConfig.getAdminConfig().getJobLogDao().findLostJobIds(clearBeforeTime);
                        if (logIds != null && logIds.size() > 0) {
                            JobAdminConfig.getAdminConfig().getJobLogDao().clearLog(logIds);
                        }
                    } while (logIds != null && logIds.size() > 0);
                    // update clean time
                    lastCleanLogTime = System.currentTimeMillis();
                }


                try {
                    TimeUnit.MINUTES.sleep(1);
                } catch (Exception e) {
                    if (!toStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
        });

        logrThread.setDaemon(true);
        logrThread.setName("job, admin JobLogReportHelper");
        logrThread.start();
    }

    public void stop() {
        toStop = true;
        // interrupt and wait
        logrThread.interrupt();
        try {
            logrThread.join();
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }
}
