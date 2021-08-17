package org.example.job.admin.complete;

import org.example.job.admin.conf.JobAdminConfig;
import org.example.job.admin.model.JobInfo;
import org.example.job.admin.model.JobLog;
import org.example.job.admin.thread.JobTriggerPoolHelper;
import org.example.job.admin.trigger.TriggerTypeEnum;
import org.example.job.admin.util.I18nUtil;
import org.example.job.core.biz.model.ReturnT;
import org.example.job.core.context.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;

public class JobCompleter {
    private static Logger logger = LoggerFactory.getLogger(JobCompleter.class);

    /**
     * common fresh handle entrance (limit only once)
     *
     * @param jobLog
     * @return
     */
    public static int updateHandleInfoAndFinish(JobLog jobLog) {
        finish(jobLog);
        if (jobLog.getHandleMsg().length() > 15000) {
            jobLog.setHandleMsg(jobLog.getHandleMsg().substring(0, 15000));
        }
        // fresh handle
        return JobAdminConfig.getAdminConfig().getJobLogDao().updateHandleInfo(jobLog);
    }

    /**
     * do somethind to finish job
     */
    public static void finish(JobLog jobLog) {
        // 1、handle success, to trigger child job
        String triggerChildMsg = null;
        if (JobContext.HANDLE_COCE_SUCCESS == jobLog.getHandleCode()) {
            JobInfo xxlJobInfo = JobAdminConfig.getAdminConfig().getJobInfoDao().loadById(jobLog.getJobId());
            if (xxlJobInfo != null && xxlJobInfo.getChildJobId() != null && xxlJobInfo.getChildJobId().trim().length() > 0) {
                triggerChildMsg = "<br><br><span style=\"color:#00c0ef;\" > >>>>>>>>>>>" + I18nUtil.getString("jobconf_trigger_child_run") + "<<<<<<<<<<< </span><br>";

                String[] childJobIds = xxlJobInfo.getChildJobId().split(",");
                for (int i = 0; i < childJobIds.length; i++) {
                    int childJobId = (childJobIds[i] != null && childJobIds[i].trim().length() > 0 && isNumeric(childJobIds[i])) ? Integer.valueOf(childJobIds[i]) : -1;
                    if (childJobId > 0) {

                        JobTriggerPoolHelper.trigger(childJobId, TriggerTypeEnum.PARENT, -1, null, null, null);
                        ReturnT<String> triggerChildResult = ReturnT.SUCCESS;

                        // add msg
                        triggerChildMsg += MessageFormat.format(I18nUtil.getString("jobconf_callback_child_msg1"),
                                (i + 1),
                                childJobIds.length,
                                childJobIds[i],
                                (triggerChildResult.getCode() == ReturnT.SUCCESS_CODE ? I18nUtil.getString("system_success") : I18nUtil.getString("system_fail")),
                                triggerChildResult.getMsg());
                    } else {
                        triggerChildMsg += MessageFormat.format(I18nUtil.getString("jobconf_callback_child_msg2"),
                                (i + 1),
                                childJobIds.length,
                                childJobIds[i]);
                    }
                }

            }
        }

        if (triggerChildMsg != null) {
            jobLog.setHandleMsg(jobLog.getHandleMsg() + triggerChildMsg);
        }
    }

    private static boolean isNumeric(String str) {
        try {
            int result = Integer.valueOf(str);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
