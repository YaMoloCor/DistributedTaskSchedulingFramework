package org.example.job.admin.thread;

import org.example.job.admin.conf.JobAdminConfig;
import org.example.job.admin.trigger.JobTrigger;
import org.example.job.admin.trigger.TriggerTypeEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class JobTriggerPoolHelper {

    private static Logger logger = LoggerFactory.getLogger(JobTriggerPoolHelper.class);

    private static JobTriggerPoolHelper instance = new JobTriggerPoolHelper();
    ;

    public static JobTriggerPoolHelper getInstance() {
        return instance;
    }

    // fast/slow thread pool
    private ThreadPoolExecutor fastTriggerPool = null;
    private ThreadPoolExecutor slowTriggerPool = null;

    public void start() {
        fastTriggerPool = new ThreadPoolExecutor(
                10,
                JobAdminConfig.getAdminConfig().getTriggerPoolFastMax(),
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(1000),
                (r) -> new Thread(r, "admin JobTriggerPoolHelper-fastTriggerPool-" + r.hashCode())
        );
        slowTriggerPool = new ThreadPoolExecutor(
                10,
                JobAdminConfig.getAdminConfig().getTriggerPoolFastMax(),
                60,
                TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>(1000),
                (r) -> new Thread(r, "admin JobTriggerPoolHelper-slowTriggerPool-" + r.hashCode())
        );
    }

    public void stop() {
        fastTriggerPool.shutdownNow();
        slowTriggerPool.shutdownNow();
        logger.info(">>>>>>>>> job trigger thread pool shutdown success.");
    }

    /**
     * @param jobId                 任务id
     * @param triggerType           手动、Cron
     * @param failRetryCount        >=0: use this param
     *                              <0: use param from job info config
     * @param executorShardingParam
     * @param executorParam         null: use job param
     *                              not null: cover job param
     * @param addressList
     */
    public static void trigger(int jobId,
                               TriggerTypeEnum triggerType,
                               int failRetryCount,
                               String executorShardingParam,
                               String executorParam,
                               String addressList) {
        instance.addTrigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
    }

    // job timeout count
    private volatile long minTim = System.currentTimeMillis() / 60000;     // ms > min
    private volatile ConcurrentHashMap<Integer, AtomicInteger> jobTimeoutCountMap = new ConcurrentHashMap<>();

    public void addTrigger(int jobId,
                           TriggerTypeEnum triggerType,
                           int failRetryCount,
                           String executorShardingParam,
                           String executorParam,
                           String addressList) {
        //选择线程池
        ThreadPoolExecutor triggerPool_ = fastTriggerPool;
        AtomicInteger jobTimeoutCount = jobTimeoutCountMap.get(jobId);
        if (jobTimeoutCount != null && jobTimeoutCount.get() > 10) {
            triggerPool_ = slowTriggerPool;
        }
        //trigger
        triggerPool_.execute(() -> {
            long start = System.currentTimeMillis();
            try {
                JobTrigger.trigger(jobId, triggerType, failRetryCount, executorShardingParam, executorParam, addressList);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            } finally {
                // check timeout-count-map
                long minTim_now = System.currentTimeMillis() / 60000;
                if (minTim != minTim_now) {
                    minTim = minTim_now;
                    jobTimeoutCountMap.clear();
                }

                // incr timeout-count-map
                long cost = System.currentTimeMillis() - start;
                if (cost > 500) {       // ob-timeout threshold 500ms
                    AtomicInteger timeoutCount = jobTimeoutCountMap.putIfAbsent(jobId, new AtomicInteger(1));
                    if (timeoutCount != null) {
                        timeoutCount.incrementAndGet();
                    }
                }
            }
        });
    }

}
