package org.example.job.admin.controller;

import org.example.job.admin.annotation.PermissionLimit;
import org.example.job.admin.conf.JobAdminConfig;
import org.example.job.core.biz.AdminBiz;
import org.example.job.core.biz.model.HandleCallbackParam;
import org.example.job.core.biz.model.RegistryParam;
import org.example.job.core.biz.model.ReturnT;
import org.example.job.core.route.JobRemotingUtil;
import org.example.job.core.util.GsonTool;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.util.List;


@Controller
@RequestMapping("/api")
public class JobApiController {
    @Resource
    AdminBiz adminBiz;
    @RequestMapping("/{uri}")
    @ResponseBody
    @PermissionLimit(limit=false)
    public ReturnT<String> api(HttpServletRequest request, @PathVariable("uri") String uri, @RequestBody(required = false) String data) {
        // valid
        if (!"POST".equalsIgnoreCase(request.getMethod())) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "invalid request, HttpMethod not support.");
        }
        if (uri==null || uri.trim().length()==0) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "invalid request, uri-mapping empty.");
        }
        if (JobAdminConfig.getAdminConfig().getAccessToken()!=null
                && JobAdminConfig.getAdminConfig().getAccessToken().trim().length()>0
                && !JobAdminConfig.getAdminConfig().getAccessToken().equals(request.getHeader(JobRemotingUtil.XXL_JOB_ACCESS_TOKEN))) {
            return new ReturnT<>(ReturnT.FAIL_CODE, "The access token is wrong.");
        }
        // services mapping
        if ("callback".equals(uri)) {
            List<HandleCallbackParam>  callbackParamList = GsonTool.fromJson(data, List.class, HandleCallbackParam.class);
            return adminBiz.callback(callbackParamList);
        }else if ("registry".equals(uri)){
            RegistryParam registryParam = GsonTool.fromJson(data, RegistryParam.class);
            return adminBiz.registry(registryParam);
        }else if ("registryRemove".equals(uri)) {
            RegistryParam registryParam = GsonTool.fromJson(data, RegistryParam.class);
            return adminBiz.registryRemove(registryParam);
        }else {
            return new ReturnT<>(ReturnT.FAIL_CODE, "invalid request, uri-mapping("+ uri +") not found.");
        }
    }
}
