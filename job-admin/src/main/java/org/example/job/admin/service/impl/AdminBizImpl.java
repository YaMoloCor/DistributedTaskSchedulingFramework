package org.example.job.admin.service.impl;

import org.example.job.admin.thread.JobCompleteHelper;
import org.example.job.admin.thread.JobRegistryHelper;
import org.example.job.core.biz.AdminBiz;
import org.example.job.core.biz.model.HandleCallbackParam;
import org.example.job.core.biz.model.RegistryParam;
import org.example.job.core.biz.model.ReturnT;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class AdminBizImpl implements AdminBiz {

    @Override
    public ReturnT<String> registry(RegistryParam registryParam) {
        return JobRegistryHelper.getInstance().registry(registryParam);
    }

    @Override
    public ReturnT<String> registryRemove(RegistryParam registryParam) {
        return JobRegistryHelper.getInstance().registryRemove(registryParam);
    }

    @Override
    public ReturnT<String> callback(List<HandleCallbackParam> callbackParamList) {
        return JobCompleteHelper.getInstance().callback(callbackParamList);
    }
}
