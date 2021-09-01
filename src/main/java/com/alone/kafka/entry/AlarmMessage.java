package com.alone.kafka.entry;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @author Administrator
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AlarmMessage {
    private String IntVersion;
    private String MsgSerial;
    private String AlarmUniqueId;
    private String NeId;
    private String NeName;
    private String SystemName;
    private String EquipmentClass;
    private String Vendor;
//    private String Version;
    private String LocateNeName;
    private String LocateNeType;
    private Date EventTime;
    private String CancelTime;
    private String DalTime;
    private String VendorAlarmType;
    private String VendorSeverity;
    private String AlarmSeverity;
    private String VendorAlarmId;
    private String AlarmStatus;

    private String AlarmTitle;

    private String ProbableCauseTxt;
    private String AlarmText;
    //    private String Specialty;
    private String AlarmLogicClass;
    private String AlarmLogicSubClass;
    private String EffectOnEquipment;
    private String EffectOnBusiness;
    private String NmsAlarmType;

    private String AlarmProvince;
    private String AlarmRegion;
    private String AlarmCounty;

    private String dt_hour;
    private String dt_day;
    private String dt_month;
    private Date insertTime;
    private Date updateTime;
}
