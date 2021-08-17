package org.example.job.admin.controller;


import org.example.job.admin.dao.JobGroupDao;
import org.example.job.admin.dao.JobInfoDao;
import org.example.job.admin.dao.JobRegistryDao;
import org.example.job.admin.model.JobGroup;
import org.example.job.admin.model.JobRegistry;
import org.example.job.admin.util.I18nUtil;
import org.example.job.core.biz.model.ReturnT;
import org.example.job.core.enums.RegistryConfig;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.*;

/**
 * 执行器管理
 */
@Controller
@RequestMapping("/jobgroup")
public class JobGroupController {
    @Resource
    JobGroupDao jobGroupDao;
    @Resource
    JobInfoDao jobInfoDao;
    @Resource
    JobRegistryDao jobRegistryDao;

    @RequestMapping
    public String index(Model model) {
        return "jobgroup/jobgroup.index";
    }

    @RequestMapping("/pageList")
    @ResponseBody
    public Map<String, Object> pageList(HttpServletRequest request,
                                        @RequestParam(required = false, defaultValue = "0") int start,
                                        @RequestParam(required = false, defaultValue = "10") int length,
                                        String appname, String title) {
        List<JobGroup> list = jobGroupDao.pageList(start, length, appname, title);
        int listCount = jobGroupDao.pageListCount(start, length, appname, title);
        Map<String, Object> res = new HashMap<>();
        res.put("recordsTotal", listCount);        // 总记录数
        res.put("recordsFiltered", listCount);    // 过滤后的总记录数
        res.put("data", list);                    // 分页列表
        return res;
    }

    @RequestMapping("/save")
    @ResponseBody
    public ReturnT<String> save(JobGroup jobGroup) {
        //验证
        if (jobGroup.getAppname() == null || jobGroup.getAppname().trim().length() == 0) {
            return new ReturnT<>(500, (I18nUtil.getString("system_please_input") + "AppName"));
        }
        if (jobGroup.getAppname().length() < 4 || jobGroup.getAppname().length() > 64) {
            return new ReturnT<>(500, I18nUtil.getString("jobgroup_field_appname_length"));
        }
        if (jobGroup.getAppname().contains("<") || jobGroup.getAppname().contains(">")) {
            return new ReturnT<>(500, "AppName" + I18nUtil.getString("system_unvalid"));
        }
        if (jobGroup.getTitle() == null || jobGroup.getTitle().trim().length() == 0) {
            return new ReturnT<>(500, (I18nUtil.getString("system_please_input") + I18nUtil.getString("jobgroup_field_title")));
        }
        if (jobGroup.getTitle().contains("<") || jobGroup.getTitle().contains(">")) {
            return new ReturnT<>(500, I18nUtil.getString("jobgroup_field_title") + I18nUtil.getString("system_unvalid"));
        }
        if (jobGroup.getAddressType() != 0) {//：0=自动注册、1=手动录入
            if (jobGroup.getAddressList() == null || jobGroup.getAddressList().trim().length() == 0) { //手动录入注册方式，机器地址不可为空
                return new ReturnT<>(500, I18nUtil.getString("jobgroup_field_addressType_limit"));
            }
            if (jobGroup.getAddressList().contains("<") || jobGroup.getAddressList().contains(">")) {//机器地址非法
                return new ReturnT<>(500, I18nUtil.getString("jobgroup_field_registryList") + I18nUtil.getString("system_unvalid"));
            }
            for (String item : jobGroup.getAddressList().split(",")) { //机器地址格式非法
                if (item == null || item.trim().length() == 0) {
                    return new ReturnT<String>(500, I18nUtil.getString("jobgroup_field_registryList_unvalid"));
                }
            }
        }
        jobGroup.setUpdateTime(new Date());

        int ret = jobGroupDao.save(jobGroup);
        return ret > 0 ? ReturnT.SUCCESS : ReturnT.FAIL;
    }

    @RequestMapping("/update")
    @ResponseBody
    public ReturnT<String> update(JobGroup jobGroup) {
        // valid
        if (jobGroup.getAppname() == null || jobGroup.getAppname().trim().length() == 0) {
            return new ReturnT<>(500, (I18nUtil.getString("system_please_input") + "AppName"));
        }
        if (jobGroup.getAppname().length() < 4 || jobGroup.getAppname().length() > 64) {
            return new ReturnT<>(500, I18nUtil.getString("jobgroup_field_appname_length"));
        }
        if (jobGroup.getTitle() == null || jobGroup.getTitle().trim().length() == 0) {
            return new ReturnT<>(500, (I18nUtil.getString("system_please_input") + I18nUtil.getString("jobgroup_field_title")));
        }
        if (jobGroup.getAddressType() == 0) {//0=自动注册、1=手动录入
            List<String> registryList = findRegistryByAppName(jobGroup.getAppname());
            String addressListStr = null;
            if (registryList != null && !registryList.isEmpty()) {
                Collections.sort(registryList);
                addressListStr = "";
                for (String item : registryList) {
                    addressListStr += item + ",";
                }
                addressListStr = addressListStr.substring(0, addressListStr.length() - 1);//去标点
                jobGroup.setAddressList(addressListStr);
            }
        } else {
            if (jobGroup.getAddressList() == null || jobGroup.getAddressList().trim().length() == 0) { //手动录入注册方式，机器地址不可为空
                return new ReturnT<>(500, I18nUtil.getString("jobgroup_field_addressType_limit"));
            }
            if (jobGroup.getAddressList().contains("<") || jobGroup.getAddressList().contains(">")) {//机器地址非法
                return new ReturnT<>(500, I18nUtil.getString("jobgroup_field_registryList") + I18nUtil.getString("system_unvalid"));
            }
            for (String item : jobGroup.getAddressList().split(",")) { //机器地址格式非法
                if (item == null || item.trim().length() == 0) {
                    return new ReturnT<String>(500, I18nUtil.getString("jobgroup_field_registryList_unvalid"));
                }
            }
        }
        jobGroup.setUpdateTime(new Date());
        int ret = jobGroupDao.update(jobGroup);
        return ret > 0 ? ReturnT.SUCCESS : ReturnT.FAIL;
    }

    private List<String> findRegistryByAppName(String appnameParam) {
        Map<String, List<String>> appAddressMap = new HashMap<>();
        List<JobRegistry> list = jobRegistryDao.findAll(RegistryConfig.DEAD_TIMEOUT, new Date());
        if (list != null) {
            for (JobRegistry item : list) {
                if (RegistryConfig.RegistType.EXECUTOR.name().equals(item.getRegistryGroup())) {
                    String appname = item.getRegistryKey();
                    List<String> registryList = appAddressMap.get(appname);
                    if (registryList == null) {
                        registryList = new ArrayList<>();
                    }
                    if (!registryList.contains(item.getRegistryValue())) {
                        registryList.add(item.getRegistryValue());
                    }
                    appAddressMap.put(appname, registryList);
                }
            }
        }
        return appAddressMap.get(appnameParam);
    }

    @RequestMapping("/remove")
    @ResponseBody
    public ReturnT<String> remove(int id) {
        //验证是否还有正在运行的任务
        int count = jobInfoDao.pageListCount(0, 10, id, 1, null, null, null);
        if (count > 0) {
            return new ReturnT<>(500, I18nUtil.getString("jobgroup_del_limit_0"));
        }
        List<JobGroup> all = jobGroupDao.findAll();
        if (all.size() == 1) {//至少保留一个执行器
            return new ReturnT<>(500, I18nUtil.getString("jobgroup_del_limit_1"));
        }
        int ret = jobGroupDao.remove(id);
        return ret > 0 ? ReturnT.SUCCESS : ReturnT.FAIL;
    }

    @RequestMapping("/loadById")
    @ResponseBody
    public ReturnT<JobGroup> loadById(int id) {
        JobGroup jobGroup = jobGroupDao.load(id);
        return jobGroup != null ? new ReturnT<>(jobGroup) : new ReturnT<>(ReturnT.FAIL_CODE, null);
    }
}
