package com.jesson.session;

/**
 * @Auther: jesson
 * @Date: 2018/8/29 00:30
 * @Description:
 */
public class WebLogConstants {
    public static final String KAFKA_SPOUT_ID = "kafkaSpoutId";
    public static final String WEB_LOG_PARSER_BOLT = "webLogParserBolt";
    public static final String COUNT_IP_BOLT = "countIpBolt";
    public static final String COUNT_BROWSER_BOLT = "countBrowserBolt";
    public static final String COUNT_OS_BOLT = "countOsBolt";

    public static final String USER_AGENT_PARSER_BOLT = "userAgentParserBolt";

    public static final String SAVE_BOLT = "saveBolt";
    public static final String PV_SUM_BOLT = "pvSumBolt";
    public static final String CV_SUM_BOLT = "cvSumBolt";



    // 流ID Session
    public  static final String IP_COUNT_STREAM = "ipCountStream";
    public  static final String URL_PARSER_STREAM = "urlParserStream";
    public  static final String HTTPREFER_PARSER_STREAM = "httpReferParserStream";
    public  static final String USERAGENT_PARSER_STREAM = "userAgentParserStream";
    public  static final String BROWSER_COUNT_STREAM = "browserCountStream";
    public  static final String OS_COUNT_STREAM = "osCountStream";
    public  static final String PV_STREAM = "pvStream";
    public  static final String CV_STREAM = "cvStream";



    // tuple key名称
    public static final String DAY = "day";
    public static final String HOUR = "hour";

    public static final String MINUTE = "minute";

    public static final String IP = "ip";
    public static final String ClientFromProvince = "clientfromprovince";
    public static final String REQUEST_URL = "requestUrl";
    public static final String HTTP_REFER = "httpRefer";
    public static final String USERAGENT = "userAgent";
    public static final String BROWSER = "browser";
    public static final String OS = "os";
    public static final String USER_SESSION = "userid";


    public static final String SERVERTIME_KPI = "serverTimeAndKpi";
    public static final String KPI_COUNTS = "kpiCounts";




    // kpi类型 这里可以统计各种指标类型 跳转率 新增用户 活跃用户等等 需要产品和研发一起沟通定义指标
    public static final String IP_KPI = "I";
    public static final String URL_KPI = "U";
    public static final String BROWSER_KPI = "B";
    public static final String OS_KPI = "O";


    // 表名称
    public static final String HBASE_TABLENAME = "nginx_log_info";
    public static final String COLUMN_FAMILY = "info";

    //zk集群配置
    public static String zkConnect = "bigdata01.com:2181,bigdata02.com:2181,bigdata03.com:2181";
}
