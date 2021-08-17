package org.example.job.admin.trigger;

import org.example.job.admin.conf.JobAdminConfig;
import org.example.job.admin.model.JobGroup;
import org.example.job.admin.model.JobInfo;
import org.example.job.admin.model.JobLog;
import org.example.job.admin.route.ExecutorRouteStrategyEnum;
import org.example.job.admin.scheduler.JobScheduler;
import org.example.job.admin.util.I18nUtil;
import org.example.job.core.biz.ExecutorBiz;
import org.example.job.core.biz.model.ReturnT;
import org.example.job.core.biz.model.TriggerParam;
import org.example.job.core.enums.ExecutorBlockStrategyEnum;
import org.example.job.core.util.IpUtil;
import org.example.job.core.util.ThrowableUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

public class JobTrigger {
    private static Logger logger = LoggerFactory.getLogger(JobTrigger.class);

    /**
     * 执行任务
     *
     * @param jobId
     * @param triggerType
     * @param failRetryCount
     * @param executorShardingParam 执行器任务分片参数，格式如 1/2
     * @param executorParam
     * @param addressList
     */
    public static void trigger(int jobId,
                               TriggerTypeEnum triggerType,
                               int failRetryCount,
                               String executorShardingParam,
                               String executorParam,
                               String addressList) {
        //  加载jobinfo
        JobInfo jobInfo = JobAdminConfig.getAdminConfig().getJobInfoDao().loadById(jobId);
        if (jobInfo == null) {
            logger.warn(">>>>>>>>>>>> trigger fail, jobId invalid，jobId={}", jobId);
            return;
        }
        // executorParam  空: 使用entity参数 不为空:  使用形参覆盖
        if (executorParam != null) {
            jobInfo.setExecutorParam(executorParam);
        }
        // failRetryCount >=0: use this param <0: use param from job info config
        int finalFailRetryCount = failRetryCount > 0 ? failRetryCount : jobInfo.getExecutorFailRetryCount();
        // 查询group
        JobGroup jobGroup = JobAdminConfig.getAdminConfig().getJobGroupDao().load(jobInfo.getJobGroup());
        // 覆盖地址
        if (addressList != null && addressList.trim().length() > 0) {
            jobGroup.setAddressType(1); //0=自动注册、1=手动录入
            jobGroup.setAddressList(addressList.trim());
        }
        // TODO sharding param 分片参数
        int[] shardingParam = null;
//        if (executorShardingParam != null) {
//            String[] shardingArr = executorShardingParam.split("/");
//            if (shardingArr.length==2 && isNumeric(shardingArr[0]) && isNumeric(shardingArr[1])) {
//                shardingParam = new int[2];
//                shardingParam[0] = Integer.valueOf(shardingArr[0]);
//                shardingParam[1] = Integer.valueOf(shardingArr[1]);
//            }
//        }
        // 判断路由策略
        if (ExecutorRouteStrategyEnum.match(jobInfo.getExecutorRouteStrategy(), null) == ExecutorRouteStrategyEnum.SHARDING_BROADCAST
                && jobGroup.getRegistryList() != null
                && !jobGroup.getRegistryList().isEmpty()
                && shardingParam == null) {//如果是分片广播
            //TODO 分片广播
        } else {
            if (shardingParam == null) {
                shardingParam = new int[]{0, 1};
            }
            processTrigger(jobGroup, jobInfo, finalFailRetryCount, triggerType, shardingParam[0], shardingParam[1]);
        }
    }

    public static void processTrigger(JobGroup group,
                                      JobInfo jobInfo,
                                      int finalFailRetryCount,
                                      TriggerTypeEnum triggerType,
                                      int index,
                                      int total) {
        ExecutorBlockStrategyEnum blockStrategyEnum = ExecutorBlockStrategyEnum.match(jobInfo.getExecutorBlockStrategy(), ExecutorBlockStrategyEnum.SERIAL_EXECUTION);
        ExecutorRouteStrategyEnum routeStrategyEnum = ExecutorRouteStrategyEnum.match(jobInfo.getExecutorRouteStrategy(), null);
        //TODO 获取分片广播参数
        //保存 jobLog
        JobLog jobLog = new JobLog();
        jobLog.setJobGroup(jobInfo.getJobGroup());
        jobLog.setJobId(jobInfo.getId());
        jobLog.setTriggerTime(new Date());
        JobAdminConfig.getAdminConfig().getJobLogDao().save(jobLog);
        logger.debug(">>>>>>>>>>> job trigger start, jobId:{}", jobLog.getId());
        //初始化trigger参数
        TriggerParam triggerParam = new TriggerParam();
        triggerParam.setJobId(jobInfo.getId());
        triggerParam.setExecutorHandler(jobInfo.getExecutorHandler());
        triggerParam.setExecutorParams(jobInfo.getExecutorParam());
        triggerParam.setExecutorBlockStrategy(jobInfo.getExecutorBlockStrategy());
        triggerParam.setExecutorTimeout(jobInfo.getExecutorTimeout());
        triggerParam.setLogId(jobLog.getId());
        triggerParam.setLogDateTime(jobLog.getTriggerTime().getTime());
        triggerParam.setGlueType(jobInfo.getGlueType());
        triggerParam.setGlueSource(jobInfo.getGlueSource());
        triggerParam.setGlueUpdatetime(jobInfo.getGlueUpdatetime().getTime());
        triggerParam.setBroadcastIndex(index);// TODO 分片参数
        triggerParam.setBroadcastTotal(total);// TODO 分片参数
        //初始化执行器地址
        String address = null;
        ReturnT<String> routeAddressResult = null;
        if (group.getRegistryList() != null && !group.getRegistryList().isEmpty()) {
            // TODO 分片
            //if (ExecutorRouteStrategyEnum.SHARDING_BROADCAST == executorRouteStrategyEnum) {
            //
            //}
            routeAddressResult = routeStrategyEnum.getRouter().route(triggerParam, group.getRegistryList());//通过路由策略 获取执行器地址
            if (routeAddressResult.getCode() == ReturnT.SUCCESS_CODE) {
                address = routeAddressResult.getContent();
            }
        } else {
            routeAddressResult = new ReturnT<>(ReturnT.FAIL_CODE, I18nUtil.getString("jobconf_trigger_address_empty"));
        }
        // tigger  远程执行
        ReturnT<String> triggerResult = null;
        if (address != null) {
            triggerResult = runExecutor(triggerParam, address);
        } else {
            triggerResult = new ReturnT<>(ReturnT.FAIL_CODE, null);
        }
        //构造日志
        StringBuffer triggerMsgSb = new StringBuffer();
        triggerMsgSb.append(I18nUtil.getString("jobconf_trigger_type")).append("：").append(triggerType.getTitle());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobconf_trigger_admin_adress")).append("：").append(IpUtil.getIp());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobconf_trigger_exe_regtype")).append("：")
                .append((group.getAddressType() == 0) ? I18nUtil.getString("jobgroup_field_addressType_0") : I18nUtil.getString("jobgroup_field_addressType_1"));
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobconf_trigger_exe_regaddress")).append("：").append(group.getRegistryList());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_executorRouteStrategy")).append("：").append(routeStrategyEnum.getTitle());
        //TODO 分片参数
//        if (shardingParam != null) {
//            triggerMsgSb.append("("+shardingParam+")");
//        }
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_executorBlockStrategy")).append("：").append(blockStrategyEnum.getTitle());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_timeout")).append("：").append(jobInfo.getExecutorTimeout());
        triggerMsgSb.append("<br>").append(I18nUtil.getString("jobinfo_field_executorFailRetryCount")).append("：").append(finalFailRetryCount);

        triggerMsgSb.append("<br><br><span style=\"color:#00c0ef;\" > >>>>>>>>>>>" + I18nUtil.getString("jobconf_trigger_run") + "<<<<<<<<<<< </span><br>")
                .append((routeAddressResult != null && routeAddressResult.getMsg() != null) ? routeAddressResult.getMsg() + "<br><br>" : "").append(triggerResult.getMsg() != null ? triggerResult.getMsg() : "");
        // 保存日志
        jobLog.setExecutorAddress(address);
        jobLog.setExecutorHandler(jobInfo.getExecutorHandler());
        jobLog.setExecutorParam(jobInfo.getExecutorParam());
        //jobLog.setExecutorShardingParam(shardingParam);
        jobLog.setExecutorFailRetryCount(finalFailRetryCount);
        jobLog.setTriggerCode(triggerResult.getCode());
        jobLog.setTriggerMsg(triggerMsgSb.toString());
        JobAdminConfig.getAdminConfig().getJobLogDao().updateTriggerInfo(jobLog);
        logger.debug(">>>>>>>>>>> job trigger end, jobId:{}", jobLog.getId());
    }

    public static ReturnT<String> runExecutor(TriggerParam triggerParam, String address) {
        ReturnT<String> runResult = null;
        try {
            ExecutorBiz executorBiz = JobScheduler.getExecutorBizClient(address);
            runResult = executorBiz.run(triggerParam);
        } catch (Exception e) {
            logger.error(">>>>>>>>>>> xxl-job trigger error, please check if the executor[{}] is running.", address, e);
            runResult = new ReturnT<>(ReturnT.FAIL_CODE, ThrowableUtil.toString(e));
        }
        StringBuffer runResultSB = new StringBuffer(I18nUtil.getString("jobconf_trigger_run") + "：");
        runResultSB.append("<br>address：").append(address);
        runResultSB.append("<br>code：").append(runResult.getCode());
        runResultSB.append("<br>msg：").append(runResult.getMsg());

        runResult.setMsg(runResultSB.toString());
        return runResult;
    }
}
