package org.example.job.admin.controller;


import org.example.job.admin.complete.JobCompleter;
import org.example.job.admin.dao.JobGroupDao;
import org.example.job.admin.dao.JobInfoDao;
import org.example.job.admin.dao.JobLogDao;
import org.example.job.admin.exception.JobException;
import org.example.job.admin.model.JobGroup;
import org.example.job.admin.model.JobInfo;
import org.example.job.admin.model.JobLog;
import org.example.job.admin.model.JobUser;
import org.example.job.admin.scheduler.JobScheduler;
import org.example.job.admin.service.LoginService;
import org.example.job.admin.thread.JobCompleteHelper;
import org.example.job.admin.util.I18nUtil;
import org.example.job.core.biz.ExecutorBiz;
import org.example.job.core.biz.model.KillParam;
import org.example.job.core.biz.model.LogParam;
import org.example.job.core.biz.model.LogResult;
import org.example.job.core.biz.model.ReturnT;
import org.example.job.core.util.DateUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.*;

/**
 * 日志管理
 */
@Controller
@RequestMapping("/joblog")
public class JobLogController {
    private static Logger logger = LoggerFactory.getLogger(JobLogController.class);
    @Resource
    JobGroupDao jobGroupDao;
    @Resource
    JobInfoDao jobInfoDao;
    @Resource
    JobLogDao jobLogDao;

    @RequestMapping
    public String index(HttpServletRequest request, Model model, @RequestParam(required = false, defaultValue = "0") Integer jobId) {
        //所有执行器
        List<JobGroup> jobGroupList_all = jobGroupDao.findAll();
        //根据权限筛选执行器
        List<JobGroup> jobGroupList = filterJobGroupByRole(request, jobGroupList_all);
        if (jobGroupList == null || jobGroupList.size() == 0) {
            throw new JobException(I18nUtil.getString("jobgroup_empty"));
        }
        model.addAttribute("JobGroupList", jobGroupList);
        // 任务
        if (jobId > 0) {
            JobInfo jobInfo = jobInfoDao.loadById(jobId);
            if (jobInfo == null) {
                throw new RuntimeException(I18nUtil.getString("jobinfo_field_id") + I18nUtil.getString("system_unvalid"));
            }

            model.addAttribute("jobInfo", jobInfo);

            // valid permission
            validPermission(request, jobInfo.getJobGroup());// 仅管理员支持查询全部；普通用户仅支持查询有权限的 jobGroup
        }
        return "joblog/joblog.index";
    }

    private List<JobGroup> filterJobGroupByRole(HttpServletRequest request, List<JobGroup> jobGroupList_all) {
        List<JobGroup> res = new ArrayList<>();
        if (jobGroupList_all != null && jobGroupList_all.size() > 0) {
            JobUser currentUser = (JobUser) request.getAttribute(LoginService.LOGIN_IDENTITY_KEY);
            if (currentUser.getRole() == 1) {
                return jobGroupList_all;
            } else {
                List<String> groupIds = new ArrayList<>();
                if (currentUser.getPermission() != null && currentUser.getPermission().trim().length() > 0) {
                    groupIds = Arrays.asList(currentUser.getPermission().trim().split(","));
                }
                for (JobGroup item : jobGroupList_all) {
                    if (groupIds.contains(String.valueOf(item.getId()))) {
                        res.add(item);
                    }
                }
            }
        }
        return res;
    }

    public void validPermission(HttpServletRequest request, int jobGroup) {
        JobUser currentUser = (JobUser) request.getAttribute(LoginService.LOGIN_IDENTITY_KEY);
        if (!currentUser.validPermission(jobGroup)) {
            throw new RuntimeException(I18nUtil.getString("system_permission_limit") + "[username=" + currentUser.getUsername() + "]");
        }
    }

    @RequestMapping("/pageList")
    @ResponseBody
    public Map<String, Object> pageList(HttpServletRequest request,
                                        @RequestParam(required = false, defaultValue = "0") int start,
                                        @RequestParam(required = false, defaultValue = "10") int length,
                                        int jobGroup, int jobId, int logStatus, String filterTime) {
        // valid permission
        validPermission(request, jobGroup);    // 仅管理员支持查询全部；普通用户仅支持查询有权限的 jobGroup

        Date triggerTimeStart = null;
        Date triggerTimeEnd = null;
        if (filterTime != null && filterTime.trim().length() > 0) {
            String[] temp = filterTime.trim().split("-");
            if (temp.length == 2) {
                triggerTimeStart = DateUtil.parseDateTime(temp[0]);
                triggerTimeEnd = DateUtil.parseDateTime(temp[1]);
            }
        }

        List<JobLog> jobLogs = jobLogDao.pageList(start, length, jobGroup, jobId, triggerTimeStart, triggerTimeEnd, logStatus);
        int pageListCount = jobLogDao.pageListCount(start, length, jobGroup, jobId, triggerTimeStart, triggerTimeEnd, logStatus);
        Map<String, Object> map = new HashMap<>();
        map.put("data", jobLogs);
        map.put("recordsFiltered", pageListCount);
        map.put("recordsTotal", pageListCount);
        return map;
    }

    @RequestMapping("/logDetailPage")
    public String logDetailPage(int id, Model model) {

        // base check
        ReturnT<String> logStatue = ReturnT.SUCCESS;
        JobLog jobLog = jobLogDao.load(id);
        if (jobLog == null) {
            throw new RuntimeException(I18nUtil.getString("joblog_logid_unvalid"));
        }

        model.addAttribute("triggerCode", jobLog.getTriggerCode());
        model.addAttribute("handleCode", jobLog.getHandleCode());
        model.addAttribute("executorAddress", jobLog.getExecutorAddress());
        model.addAttribute("triggerTime", jobLog.getTriggerTime().getTime());
        model.addAttribute("logId", jobLog.getId());
        return "joblog/joblog.detail";
    }

    @RequestMapping("/logDetailCat")
    @ResponseBody
    public ReturnT<LogResult> logDetailCat(String executorAddress, long triggerTime, long logId, int fromLineNum) {
        try {
            ExecutorBiz executorBiz = JobScheduler.getExecutorBizClient(executorAddress);
            ReturnT<LogResult> logResult = executorBiz.log(new LogParam(triggerTime, logId, fromLineNum));

            // is end
            if (logResult.getContent() != null && logResult.getContent().getFromLineNum() > logResult.getContent().getToLineNum()) {
                JobLog jobLog = jobLogDao.load(logId);
                if (jobLog.getHandleCode() > 0) {
                    logResult.getContent().setEnd(true);
                }
            }

            return logResult;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return new ReturnT<>(ReturnT.FAIL_CODE, e.getMessage());
        }
    }

    @RequestMapping("/logKill")
    @ResponseBody
    public ReturnT<String> logKill(int id) {
        // base check
        JobLog jobLog = jobLogDao.load(id);
        JobInfo jobInfo = jobInfoDao.loadById(jobLog.getJobId());
        if (jobInfo == null) {
            return new ReturnT<>(500, I18nUtil.getString("jobinfo_glue_jobid_unvalid"));
        }
        if (ReturnT.SUCCESS_CODE != jobLog.getTriggerCode()) {
            return new ReturnT<>(500, I18nUtil.getString("joblog_kill_log_limit"));
        }
        // request of kill
        ReturnT<String> runResult = null;
        try {
            ExecutorBiz executorBiz = JobScheduler.getExecutorBizClient(jobLog.getExecutorAddress());
            runResult = executorBiz.kill(new KillParam(jobInfo.getId()));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            runResult = new ReturnT<>(500, e.getMessage());
        }

        if (ReturnT.SUCCESS_CODE == runResult.getCode()) {
            jobLog.setHandleCode(ReturnT.SUCCESS_CODE);
            jobLog.setHandleMsg(I18nUtil.getString("joblog_kill_log_byman") + ":" + (runResult.getMsg() != null ? runResult.getMsg() : ""));
            jobLog.setHandleTime(new Date());
            JobCompleter.updateHandleInfoAndFinish(jobLog);
            return new ReturnT<>(runResult.getMsg());
        } else {
            return new ReturnT<>(500, runResult.getMsg());
        }
    }

    @RequestMapping("/clearLog")
    @ResponseBody
    public ReturnT<String> clearLog(int jobGroup, int jobId, int type){
        Date clearBeforeTime = null;
        int clearBeforeNum = 0;
        if (type == 1) {
            clearBeforeTime = DateUtil.addMonths(new Date(), -1);	// 清理一个月之前日志数据
        } else if (type == 2) {
            clearBeforeTime = DateUtil.addMonths(new Date(), -3);	// 清理三个月之前日志数据
        } else if (type == 3) {
            clearBeforeTime = DateUtil.addMonths(new Date(), -6);	// 清理六个月之前日志数据
        } else if (type == 4) {
            clearBeforeTime = DateUtil.addYears(new Date(), -1);	// 清理一年之前日志数据
        } else if (type == 5) {
            clearBeforeNum = 1000;		// 清理一千条以前日志数据
        } else if (type == 6) {
            clearBeforeNum = 10000;		// 清理一万条以前日志数据
        } else if (type == 7) {
            clearBeforeNum = 30000;		// 清理三万条以前日志数据
        } else if (type == 8) {
            clearBeforeNum = 100000;	// 清理十万条以前日志数据
        } else if (type == 9) {
            clearBeforeNum = 0;			// 清理所有日志数据
        } else {
            return new ReturnT<>(ReturnT.FAIL_CODE, I18nUtil.getString("joblog_clean_type_unvalid"));
        }

        List<Long> logIds = null;
        do {
            logIds = jobLogDao.findClearLogIds(jobGroup, jobId, clearBeforeTime, clearBeforeNum, 1000);
            if (logIds!=null && logIds.size()>0) {
                jobLogDao.clearLog(logIds);
            }
        } while (logIds!=null && logIds.size()>0);

        return ReturnT.SUCCESS;
    }
}
