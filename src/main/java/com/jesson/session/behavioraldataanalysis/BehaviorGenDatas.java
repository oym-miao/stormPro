package com.jesson.session.behavioraldataanalysis;


import com.jesson.session.kafka.MyKafkaProducer;
import com.jesson.session.topmerchant.MerchantDatas;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * @Auther: wuyue
 * @Date: 2018/8/18 17:22
 * @Description: 制作假数据 pv/uv
 */
public class BehaviorGenDatas {

    //访问url
    private static final String[] ACCESS_URL = { "/jiaoyue/11.html", "/yule/12.htm", "/yingshi/33.html", "/jianshen/121.html", "/zhengzhi/99.html",
            "/guojixinwen/11.html", "/IT/139.html", "/JAVA/991.html", "/bigdata/77.html","/AI/185.html", "/javascript/88.html" };
    //ip数据
    private static final String[] IP = { "27.128.172.33", "42.86.2.221", "36.5.133.222", "14.18.55.21", "1.82.127.22","14.196.255.220",
            "36.101.255.11","27.16.124.21","161.190.25.25" };

    //为了测试的数值均一性，我们直接写死几个城市，方便看Storm的结果
    private static final String[] CITY = { "上海", "北京", "合肥", "重庆", "香港","澳门","兰州","深圳"};

    //HTTP CODE
    private static final int[] HTTP_CODE={200,404,500,305,400,401,501,502};

    //referer https://www.google.com/search?oq=bigdata 判断目标网站是哪个搜索引擎导流
    private static final String[] REFERER_URL ={"https://www.google.com/search?oq=",
            "https://www.baidu.com/s?wd=","https://cn.bing.com/search?q=","https://www.sogou.com/web?query=",
            "https://www.so.com/s?q=","https://search.yahoo.com/search?p="};

    // search keywords
    private static final String[] KEYWORDS = { "大数据", "范冰冰", "Java未来", "Android未来在哪里", "周杰伦",
            "Spark技术", "Storm和Hbase集成环境开发", "架构师培训", "菜鸟窝Jesson大牛", "前端技术"};
    // client type
    private static final String[] CLIENT_TYPE = { "Android_HAWEI7300", "Iphone6S", "Iphone6", "IphoneX", "Iphone4S",
            "MacPro2018","MacPro2017","MacPro2011","Windows"};

    // click area
    private static final String[] CLICK_AREA = { "京东海外淘", "京东文具", "京东购物", "京东生鲜", "京东金融", "京东贷款"};

    //userid
    private static final String[] USER_ID = { "0001", "0003","7788","0077","1123","9991","0010","8811","6612","2209"};

    //模拟订单商家信息
    private static final String[] Merchant_Name = { "优衣库","天猫","淘宝","咕噜大大","快乐宝贝","跑男","路易斯威登", "Apple","BMW",
            "华为","绿联数码","三星电子","哈喽kitty"};

    /**
     * 模拟生成网站PV/CV统计模拟记录
     *
     * @return
     */
    public static String generateCustomerRecord() {
        long timestamp = System.currentTimeMillis();
        StringBuilder stringBuilder = new StringBuilder("\""+timestamp + "\"");
        Random r = new Random();
        //用户id
        String user_id = USER_ID[r.nextInt(USER_ID.length)];
        stringBuilder.append(" \"" + user_id + "\"");

        String access_url = ACCESS_URL[r.nextInt(ACCESS_URL.length)];
        stringBuilder.append(" \"" + access_url + "\"");

//        String ip = IP[r.nextInt(IP.length)];
//        stringBuilder.append(" \"" + ip + "\"");

        String city = CITY[r.nextInt(CITY.length)];
        stringBuilder.append(" \"" + city + "\"");

        int http_code = HTTP_CODE[r.nextInt(HTTP_CODE.length)];
        stringBuilder.append(" \"" + http_code + "\"");

        String referer_url = REFERER_URL[r.nextInt(REFERER_URL.length)]+KEYWORDS[r.nextInt(KEYWORDS.length)];
        stringBuilder.append(" \"" + referer_url  + "\"");

        String client_type = CLIENT_TYPE[r.nextInt(CLIENT_TYPE.length)];
        stringBuilder.append(" \"" + client_type + "\"");

        String click_area = CLICK_AREA[r.nextInt(CLICK_AREA.length)];
        stringBuilder.append(" \"" + click_area + "\"");


        stringBuilder.append(" \"" + r.nextInt(100) + "\""); //总金额
        stringBuilder.append(" \"" + r.nextInt(99) + "\""); //优惠金额

        String merchant_name = Merchant_Name[r.nextInt(Merchant_Name.length)];
        stringBuilder.append(" \"" + merchant_name + "\"");

        System.err.println(stringBuilder.toString());
        return stringBuilder.toString();
    }

    /**
     *
     * 功能描述: 把模拟的数据写到文件
     *
     * @param:
     * @return:
     * @auther: wuyue
     * @date: 2018/9/4 下午4:07
     */
    public static void wirteLoginfo2File() throws IOException{
        File writename = new File("abc.log"); // 相对路径，如果没有则要建立一个新的output。txt文件
        if(!writename.exists()){
            writename.createNewFile(); // 创建新文件
        }
        BufferedWriter out = new BufferedWriter(new FileWriter(writename,true));
        for (int i=0;i<100;i++) {
            out.write(generateCustomerRecord()+"\r\n"); //\r\n换行
        }
        out.flush(); // 把缓存区内容压入文件
        out.close(); // 最后记得关闭文件
    }

    /**
     *
     * 功能描述: 把loginfo信息发送到kafka集群上去
     *
     * @param:
     * @return:
     * @auther: wuyue
     * @date: 2018/9/4 下午4:08
     */
    public static void kafkaProducer2server(){
        MyKafkaProducer kafkaProducer = new MyKafkaProducer();
        Producer<String,String> producer = kafkaProducer.getKafkaProducer("192.168.105.129:9092");
        for (; ; ) {
            try {
                Thread.sleep(600);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            String msgKey = System.currentTimeMillis()+ "";
            String msg = BehaviorGenDatas.generateCustomerRecord();
            // 如果topic不存在，则会自动创建，默认replication-factor为1，partitions为0
            KeyedMessage<String, String> data = kafkaProducer.getKeyedMessage("order_top6", msgKey, msg);
            kafkaProducer.sendMassage(producer, data);
        }
    }
    /**
     * 创建生产者 在服务器上进行处理
     * @param args
     */
    public static void main(String[] args) throws IOException {
  //      wirteLoginfo2File();
        kafkaProducer2server();
    }

}
