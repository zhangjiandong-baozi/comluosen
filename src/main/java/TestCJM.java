

import bean.IPAccessBean;
import com.aliyun.openservices.log.flink.ConfigConstants;
import com.aliyun.openservices.log.flink.FlinkLogConsumer;
import com.aliyun.openservices.log.flink.data.RawLog;
import com.aliyun.openservices.log.flink.data.RawLogGroup;
import com.aliyun.openservices.log.flink.data.RawLogGroupList;
import com.aliyun.openservices.log.flink.data.RawLogGroupListDeserializer;
import org.apache.commons.cli.*;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import sun.tools.jar.resources.jar;


import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Properties;

/**
 * @author
 * @create 2021-12-12
 */
public class TestCJM {



    public static void main(String[] args) throws ParseException {



        //参考实例 https://help.aliyun.com/document_detail/207700.html
        //参考代码 https://github.com/aliyun/alibabacloud-hologres-connectors/blob/master/hologres-connector-examples/hologres-connector-flink-examples/src/main/java/com/alibaba/ververica/connectors/hologres/example/FlinkDSAndSQLToHoloExample.java
        //准备工作如下： 即将hologres-connector-flink-base install 上传到本地maven仓库，将flinkconnectors 1.11 或者 flinkconnectors 1.12 install 上传到本地仓库
//        build base jar 并 install 到本地maven仓库
//        -P指定相关版本参数
//        mvn install -pl hologres-connector-flink-base clean package -DskipTests -Pflink-1.11
//        build jar
//        mvn install -pl hologres-connector-flink-1.11 clean package -DskipTests


         //连接sls的连接信息 类似于jdbc
        final Properties props = new Properties();
         String endpoint = "";
         String group ="";
         String accessKeyId="";
         String accessKey ="";
         String project="";
         String logStore="";
         int interval = 100;

        props.put(ConfigConstants.LOG_ENDPOINT, endpoint);
        props.put(ConfigConstants.LOG_CONSUMERGROUP, group);
        props.put(ConfigConstants.LOG_ACCESSSKEYID, accessKeyId);
        props.put(ConfigConstants.LOG_ACCESSKEY, accessKey);
        props.put(ConfigConstants.LOG_PROJECT, project);
        props.put(ConfigConstants.LOG_LOGSTORE, logStore);
        props.put(ConfigConstants.LOG_FETCH_DATA_INTERVAL_MILLIS, interval);

        //构建flink source
        RawLogGroupListDeserializer deserializer = new RawLogGroupListDeserializer();
        FlinkLogConsumer<RawLogGroupList> slsSource = new FlinkLogConsumer<>(deserializer, props);


        EnvironmentSettings.Builder streamBuilder =
        EnvironmentSettings.newInstance().inStreamingMode();
        //构建flink流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //构建flinksql处理环境
        StreamTableEnvironment tEnv =
                StreamTableEnvironment.create(env, streamBuilder.useBlinkPlanner().build());

        //将你要的sls的数据清洗出来
        DataStreamSource<RawLogGroupList> slsInputDS = env.addSource(slsSource);
        SingleOutputStreamOperator<IPAccessBean> inputDS = slsInputDS.flatMap(new FlatMapFunction<RawLogGroupList, IPAccessBean>() {
            @Override
            public void flatMap(RawLogGroupList rawLogGroupList, Collector<IPAccessBean> collector) throws Exception {
                  try{
                    List<RawLogGroup> rawLogGroups = rawLogGroupList.getRawLogGroups();
                    for (RawLogGroup rawLogGroup : rawLogGroups) {
                        List<RawLog> logs = rawLogGroup.getLogs();
                        for (RawLog log : logs) {

                            //以下是sls的数据格式，将有用的数据筛选出来
                            if (log.getContents() != null && log.getContents().get("uri") != null && log.getContents().get("http_request_id") != null) {
                                String clientip = log.getContents().get("clientip");
                                String uri = log.getContents().get("uri");
                                String request_url = log.getContents().get("request_url");
                                String http_request_id = log.getContents().get("http_request_id");
                                String times = log.getContents().get("@timestamp");
                                String[] split = times.split("/+");
                                String[] ts = split[0].split("T");
                                times = ts[0] + " " + ts[1];
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                long timestamp = sdf.parse(times).getTime();
                                IPAccessBean ipAccessBean = new IPAccessBean(clientip, uri, request_url, http_request_id, timestamp);
                                collector.collect(ipAccessBean);
                            }
                        }
                    }
                } catch (Exception e) {
                    System.out.println("bean 转换有问题 " + e);
                }
            }
        });

        // flink 流转换成表
        Table inputTable =
        tEnv.fromDataStream(
                inputDS,
                "clientIp as client_ip,uri,requestUrl,httpRequestId,timestamp as ts"
                );


        //连接hologress库的连接信息    类似于jdbc的那种
//        connector	是	结果表类型，固定值为hologres。
//        dbname	是	Hologres的数据库名称。
//        tablename	是	Hologres接收数据的表名称。
//        username	是	当前阿里云账号的AccessKey ID。
//        您可以单击AccessKey 管理，获取AccessKey ID。
//
//        password	是	当前阿里云账号的AccessKey Secret。
//        您可以单击AccessKey 管理，获取AccessKey Secret。
//
//        endpoint	是	Hologres的VPC网络地址。进入Hologres管理控制台的实例详情页，从实例配置获取Endpoint。
        Options options = new Options();
        options.addOption("e", "endpoint", true, "Hologres endpoint");
        options.addOption("u", "username", true, "Username");
        options.addOption("p", "password", true, "Password");
        options.addOption("d", "database", true, "Database");
        options.addOption("t", "tablename", true, "Table name");

        CommandLineParser parser = new DefaultParser();
        CommandLine commandLine = parser.parse(options, args);
        String endPoint = commandLine.getOptionValue("endpoint");
        String userName = commandLine.getOptionValue("username");
        String password = commandLine.getOptionValue("password");
        String database = commandLine.getOptionValue("database");
        String tableName = commandLine.getOptionValue("tablename");



        String createHologresTable =
        String.format(
                "create table sink1("
                        + "  client_ip bigint,"
                        + "  uri string,"
                        + "  requestUrl string,"
                        + "  httpRequestId string,"
                        + "  ts timestamp"
                        + ") with ("
                        + "  'connector'='hologres',"
                        + "  'dbname' = '%s',"
                        + "  'tablename' = '%s',"
                        + "  'username' = '%s',"
                        + "  'password' = '%s',"
                        + "  'endpoint' = '%s'"
                        + ")",
                database, tableName, userName, password, endPoint);
        //创建表
        tEnv.executeSql(createHologresTable);
        //实时写入hologress
        tEnv.executeSql("insert into sink1 select * from " + inputTable);


    }
}
