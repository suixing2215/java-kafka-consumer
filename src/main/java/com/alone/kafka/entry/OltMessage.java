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
public class OltMessage {
    private String ProvinceID;
    private String IntVersion;
    private String MsgSerial;
    private String AlarmUniqueId;
    private String ClearId;
    private String StandardFlag;
    private String SubAlarmType;
    private String NeId;
    private String LocateNeName;
    private String LocateNeType;
    private String NeName;
    private String NeAlias;
    private String EquipmentClass;
    private String NeIp;
    private String SystemName;
    private String Vendor;
    private String Version;
    private String LocateNeStatus;
    private String ProjectNo;
    private String ProjectName;
    private String ProjectStartTime;
    private String ProjectEndTime;
    private String LocateInfo;
    private Date EventTime;
    private String CancelTime;
    private String DalTime;
    private String VendorAlarmType;
    private String VendorSeverity;
    private String AlarmSeverity;
    private String VendorAlarmId;
    private String NmsAlarmId;
    private String AlarmStatus;
    private String AckFlag;
    private String AckTime;
    private String AckUser;
    private String AlarmTitle;
    private String StandardAlarmName;
    private String ProbableCauseTxt;
    private String AlarmText;
    private String CircuitNo;
    private String PortRate;
    private String Specialty;
    private String BusinessSystem;
    private String AlarmLogicClass;
    private String AlarmLogicSubClass;
    private String EffectOnEquipment;
    private String EffectOnBusiness;
    private String NmsAlarmType;
    private String SendGroupFlag;
    private String RelatedFlag;
    private String AlarmProvince;
    private String AlarmRegion;
    private String AlarmCounty;
    private String Site;
    private String AlarmActCount;
    private String CorrelateAlarmFlag;
    private String SheetSendStatus;
    private String SheetStatus;
    private String SheetNo;
    private String AlarmMemo;
    private String dt_day;
    private String dt_month;
    private String dt_hour;
    private Date insertTime;
    private Date updateTime;
}
