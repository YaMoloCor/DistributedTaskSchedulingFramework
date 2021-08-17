package org.example.job.admin.trigger;

import org.example.job.admin.util.I18nUtil;

/**
 * jobconf_trigger_type_cron=Cron触发
 * jobconf_trigger_type_manual=手动触发
 * jobconf_trigger_type_parent=父任务触发
 * jobconf_trigger_type_api=API触发
 * jobconf_trigger_type_retry=失败重试触发
 * jobconf_trigger_type_misfire=调度过期补偿
 */
public enum TriggerTypeEnum {
    MANUAL(I18nUtil.getString("jobconf_trigger_type_manual")),
    CRON(I18nUtil.getString("jobconf_trigger_type_cron")),
    RETRY(I18nUtil.getString("jobconf_trigger_type_retry")),
    PARENT(I18nUtil.getString("jobconf_trigger_type_parent")),
    API(I18nUtil.getString("jobconf_trigger_type_api")),
    MISFIRE(I18nUtil.getString("jobconf_trigger_type_misfire"));

    private TriggerTypeEnum(String title){
        this.title = title;
    }
    private String title;
    public String getTitle() {
        return title;
    }
}
