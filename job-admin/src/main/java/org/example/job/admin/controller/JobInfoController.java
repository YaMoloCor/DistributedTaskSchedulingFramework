package org.example.job.admin.controller;


import org.example.job.admin.dao.JobGroupDao;
import org.example.job.admin.exception.JobException;
import org.example.job.admin.model.JobGroup;
import org.example.job.admin.model.JobInfo;
import org.example.job.admin.model.JobUser;
import org.example.job.admin.route.ExecutorRouteStrategyEnum;
import org.example.job.admin.scheduler.MisfireStrategyEnum;
import org.example.job.admin.scheduler.ScheduleTypeEnum;
import org.example.job.admin.service.JobService;
import org.example.job.admin.service.LoginService;
import org.example.job.admin.thread.JobScheduleHelper;
import org.example.job.admin.thread.JobTriggerPoolHelper;
import org.example.job.admin.trigger.TriggerTypeEnum;
import org.example.job.admin.util.I18nUtil;
import org.example.job.core.biz.model.ReturnT;
import org.example.job.core.enums.ExecutorBlockStrategyEnum;
import org.example.job.core.glue.GlueTypeEnum;
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
 * 任务管理
 */
@Controller
@RequestMapping("/jobinfo")
public class JobInfoController {

    private static Logger logger = LoggerFactory.getLogger(JobInfoController.class);

    @Resource
    private JobGroupDao jobGroupDao;

    @Resource
    private JobService jobService;

    @RequestMapping
    public String index(HttpServletRequest request, Model model, @RequestParam(required = false, defaultValue = "-1") int jobGroup) {
        // 枚举-字典
        model.addAttribute("ExecutorRouteStrategyEnum", ExecutorRouteStrategyEnum.values());	    // 路由策略-列表
        model.addAttribute("GlueTypeEnum", GlueTypeEnum.values());								// Glue类型-字典
        model.addAttribute("ExecutorBlockStrategyEnum", ExecutorBlockStrategyEnum.values());	    // 阻塞处理策略-字典
        model.addAttribute("ScheduleTypeEnum", ScheduleTypeEnum.values());	    				// 调度类型
        model.addAttribute("MisfireStrategyEnum", MisfireStrategyEnum.values());	    			// 调度过期策略
        // 执行器列表
        List<JobGroup> jobGroupList_all = jobGroupDao.findAll();
        // filter group
        List<JobGroup> jobGroupList = filterJobGroupByRole(request, jobGroupList_all);
        if (jobGroupList == null || jobGroupList.size() == 0) {
            throw new JobException(I18nUtil.getString("jobgroup_empty"));
        }
        model.addAttribute("JobGroupList", jobGroupList);
        model.addAttribute("jobGroup", jobGroup);
        return "jobinfo/jobinfo.index";
    }

    private List<JobGroup> filterJobGroupByRole(HttpServletRequest request, List<JobGroup> jobGroupList_all) {
        List<JobGroup> jobGroupList = new ArrayList<>();
        if (jobGroupList_all!=null && jobGroupList_all.size()>0) {
            JobUser loginUser = (JobUser) request.getAttribute(LoginService.LOGIN_IDENTITY_KEY);
            if (loginUser.getRole() == 1) {
                jobGroupList = jobGroupList_all;
            } else {
                List<String> groupIdStrs = new ArrayList<>();
                if (loginUser.getPermission()!=null && loginUser.getPermission().trim().length()>0) {
                    groupIdStrs = Arrays.asList(loginUser.getPermission().trim().split(","));
                }
                for (JobGroup groupItem:jobGroupList_all) {
                    if (groupIdStrs.contains(String.valueOf(groupItem.getId()))) {
                        jobGroupList.add(groupItem);
                    }
                }
            }
        }
        return jobGroupList;
    }

    public static void validPermission(HttpServletRequest request, int jobGroup) {
        JobUser loginUser = (JobUser) request.getAttribute(LoginService.LOGIN_IDENTITY_KEY);
        if (!loginUser.validPermission(jobGroup)) {
            throw new RuntimeException(I18nUtil.getString("system_permission_limit") + "[username="+ loginUser.getUsername() +"]");
        }
    }
    @RequestMapping("/pageList")
    @ResponseBody
    public Map<String, Object> pageList(@RequestParam(required = false, defaultValue = "0") int start,
                                        @RequestParam(required = false, defaultValue = "10") int length,
                                        int jobGroup, int triggerStatus, String jobDesc, String executorHandler, String author) {

        return jobService.pageList(start, length, jobGroup, triggerStatus, jobDesc, executorHandler, author);
    }

    @RequestMapping("/add")
    @ResponseBody
    public ReturnT<String> add(JobInfo jobInfo) {
        return jobService.add(jobInfo);
    }

    @RequestMapping("/update")
    @ResponseBody
    public ReturnT<String> update(JobInfo jobInfo) {
        return jobService.update(jobInfo);
    }

    @RequestMapping("/remove")
    @ResponseBody
    public ReturnT<String> remove(int id) {
        return jobService.remove(id);
    }

    @RequestMapping("/stop")
    @ResponseBody
    public ReturnT<String> pause(int id) {
        return jobService.stop(id);
    }

    @RequestMapping("/start")
    @ResponseBody
    public ReturnT<String> start(int id) {
        return jobService.start(id);
    }

    /**
     * 执行一次
     * @param id
     * @param executorParam
     * @param addressList
     * @return
     */
    @RequestMapping("/trigger")
    @ResponseBody
    //@PermissionLimit(limit = false)
    public ReturnT<String> triggerJob(int id, String executorParam, String addressList) {
        // force cover job param
        if (executorParam == null) {
            executorParam = "";
        }

        JobTriggerPoolHelper.trigger(id, TriggerTypeEnum.MANUAL, -1, null, executorParam, addressList);
        return ReturnT.SUCCESS;
    }

    /**
     * 查询下次执行时间
     * @param scheduleType
     * @param scheduleConf
     * @return
     */
    @RequestMapping("/nextTriggerTime")
    @ResponseBody
    public ReturnT<List<String>> nextTriggerTime(String scheduleType, String scheduleConf) {

        JobInfo paramJobInfo = new JobInfo();
        paramJobInfo.setScheduleType(scheduleType);
        paramJobInfo.setScheduleConf(scheduleConf);

        List<String> result = new ArrayList<>();
        try {
            Date lastTime = new Date();
            for (int i = 0; i < 5; i++) {
                lastTime = JobScheduleHelper.generateNextValidTime(paramJobInfo, lastTime);
                if (lastTime != null) {
                    result.add(DateUtil.formatDateTime(lastTime));
                } else {
                    break;
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return new ReturnT<>(ReturnT.FAIL_CODE, (I18nUtil.getString("schedule_type")+I18nUtil.getString("system_unvalid")) + e.getMessage());
        }
        return new ReturnT<>(result);

    }
}
