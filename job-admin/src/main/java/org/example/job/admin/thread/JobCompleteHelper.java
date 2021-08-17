package org.example.job.admin.thread;

import jdk.nashorn.internal.scripts.JO;
import org.example.job.admin.complete.JobCompleter;
import org.example.job.admin.conf.JobAdminConfig;
import org.example.job.admin.model.JobLog;
import org.example.job.admin.util.I18nUtil;
import org.example.job.core.biz.model.HandleCallbackParam;
import org.example.job.core.biz.model.ReturnT;
import org.example.job.core.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.concurrent.*;

public class JobCompleteHelper {
    private static Logger logger = LoggerFactory.getLogger(JobCompleteHelper.class);

    private static JobCompleteHelper instance = new JobCompleteHelper();

    public static JobCompleteHelper getInstance() {
        return instance;
    }

    private ThreadPoolExecutor callbackThreadPool = null;
    private Thread monitorThread;
    private volatile boolean toStop = false;

    public void start() {
        // for callback
        callbackThreadPool = new ThreadPoolExecutor(
                2,
                20,
                30L,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<>(3000),
                r -> new Thread(r, "job admin JobLosedMonitorHelper-callbackThreadPool-" + r.hashCode()),
                (r, executor) -> {
                    r.run();
                    logger.warn(">>>>>>>>>>> job callback too fast, match threadpool rejected handler(run now).");
                });
        monitorThread = new Thread(() -> {
            // wait for JobTriggerPoolHelper-init
            try {
                TimeUnit.MILLISECONDS.sleep(50);
            } catch (InterruptedException e) {
                if (!toStop) {
                    logger.error(e.getMessage(), e);
                }
            }
            while (!toStop) {
                try {
                    // 任务结果丢失处理：调度记录停留在 "运行中" 状态超过10min，且对应执行器心跳注册失败不在线，则将本地调度主动标记失败；
                    Date lostTime = DateUtil.addMonths(new Date(), -10);
                    List<Long> lostJobIds = JobAdminConfig.getAdminConfig().getJobLogDao().findLostJobIds(lostTime);//查找调度记录停留在 "运行中" 状态超过10min
                    if (lostJobIds != null && lostJobIds.size() > 0) {
                        for (Long jobId : lostJobIds) {
                            JobLog jobLog = new JobLog();
                            jobLog.setId(jobId);
                            jobLog.setHandleTime(new Date());
                            jobLog.setHandleCode(ReturnT.FAIL_CODE);
                            jobLog.setHandleMsg(I18nUtil.getString("joblog_lost_fail"));
                            JobCompleter.updateHandleInfoAndFinish(jobLog);
                        }
                    }
                } catch (Exception e) {
                    if (!toStop) {
                        logger.error(">>>>>>>>>>> job, job fail monitor thread error:{}", e);
                    }
                }
                try {
                    TimeUnit.SECONDS.sleep(60);
                } catch (Exception e) {
                    if (!toStop) {
                        logger.error(e.getMessage(), e);
                    }
                }
            }
            logger.info(">>>>>>>>>>> job, JobLosedMonitorHelper stop");
        });
        monitorThread.setName("job, admin JobLosedMonitorHelper");
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    public void stop() {
        toStop = true;

        // stop registryOrRemoveThreadPool
        callbackThreadPool.shutdownNow();

        // stop monitorThread (interrupt and wait)
        monitorThread.interrupt();
        try {
            monitorThread.join();
        } catch (InterruptedException e) {
            logger.error(e.getMessage(), e);
        }
    }
    // ---------------------- helper ----------------------

    public ReturnT<String> callback(List<HandleCallbackParam> callbackParamList) {
        callbackThreadPool.execute(() -> {
            for (HandleCallbackParam handleCallbackParam : callbackParamList) {
                ReturnT<String> callbackResult = callback(handleCallbackParam);
                logger.debug(">>>>>>>>> JobApiController.callback {}, handleCallbackParam={}, callbackResult={}",
                        (callbackResult.getCode() == ReturnT.SUCCESS_CODE ? "success" : "fail"), handleCallbackParam, callbackResult);
            }
        });
        return ReturnT.SUCCESS;
    }

    private ReturnT<String> callback(HandleCallbackParam handleCallbackParam) {
        JobLog jobLog = JobAdminConfig.getAdminConfig().getJobLogDao().load(handleCallbackParam.getLogId());
        if (jobLog == null) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "log item not found.");
        }
        if (jobLog.getHandleCode() > 0) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "log repeate callback.");     // avoid repeat callback, trigger child job etc
        }
        // handle msg
        StringBuffer handleMsg = new StringBuffer();
        if (jobLog.getHandleMsg() != null) {
            handleMsg.append(jobLog.getHandleMsg()).append("<br>");
        }
        if (handleCallbackParam.getHandleMsg() != null) {
            handleMsg.append(handleCallbackParam.getHandleMsg());
        }

        // success, save log
        jobLog.setHandleTime(new Date());
        jobLog.setHandleCode(handleCallbackParam.getHandleCode());
        jobLog.setHandleMsg(handleMsg.toString());
        JobCompleter.updateHandleInfoAndFinish(jobLog);
        return ReturnT.SUCCESS;
    }
}
