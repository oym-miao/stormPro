package com.jesson.session.utils;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;

import java.io.IOException;

/**
 * @Auther: jesson
 * @Date: 2018/8/29 00:50
 * @Description:
 */
public class UserAgentUtil {
    //static UserAgentInfo userAgentInfo = new UserAgentInfo();
    static UASparser uasParser = null;
    static {
        try {
            uasParser = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public static UserAgentInfo analyticUserAgent(String userAgent) {

        if(userAgent!=null&!"".equals(userAgent)){
            UserAgentInfo userAgentInfo = null;
            try {
                userAgentInfo = UserAgentUtil.uasParser.parse(userAgent);
            } catch (IOException e) {
                e.printStackTrace();
            }


            return userAgentInfo;
        }

        return null;
    }

    //"Mozilla/5.0 (Windows NT 6.1; WOW64; rv:40.0) Gecko/20100101 Firefox/40.1"


//    public static class UserAgentInfo {
//        private String OsName;
//        private String OsVersion;
//        private String BrowserName;
//        private String BrowserVersion;
//
//        public String getOsName() {
//            return OsName;
//        }
//
//        public void setOsName(String osName) {
//            OsName = osName;
//        }
//
//        public String getOsVersion() {
//            return OsVersion;
//        }
//
//        public void setOsVersion(String osVersion) {
//            OsVersion = osVersion;
//        }
//
//        public String getBrowserName() {
//            return BrowserName;
//        }
//
//        public void setBrowserName(String browserName) {
//            BrowserName = browserName;
//        }
//
//        public String getBrowserVersion() {
//            return BrowserVersion;
//        }
//
//        public void setBrowserVersion(String browserVersion) {
//            BrowserVersion = browserVersion;
//        }
//    }

}
