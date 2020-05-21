package org.nlpcn.es4sql;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_CONNECTIONPROPERTIES;
import static com.alibaba.druid.pool.DruidDataSourceFactory.PROP_URL;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.ElasticSearchDruidDataSourceFactory;

/**
 * Created by allwefantasy on 8/18/16.
 */
public class Test {
    public static String sqlToEsQuery(String sql) throws Exception {
        Map actions = new HashMap();
        Settings settings = Settings.builder().build();
//        Client client = new NodeClient(settings, null, null, actions);
//        Settings.builder()
//                .put(ThreadContext.PREFIX + ".key1", "val1")
//                .put(ThreadContext.PREFIX + ".key2", "val 2")
//                .build();

        ThreadPool threadPool = new ThreadPool(settings);
        Client client = new NodeClient(settings, threadPool);
        SearchDao searchDao = new org.nlpcn.es4sql.SearchDao(client);
        try {
            return searchDao.explain(sql).explain().explain();
        } catch (Exception e) {
            throw e;
        }
    }

    public static void main(String[] args) throws Exception {
        // String sql = "SELECT u as u2,count(distinct(mid)) as count FROM panda_quality where ty='buffer' and day='20160816' and tm>1471312800.00 and tm<1471313100.00 and domain='http://pl10.live.panda.tv' group by u  order by count desc limit 5000";
//        sql = "SELECT sum(num) as num2,newtype as nt  from  twitter2 group by nt  order by num2 ";
//        System.out.println("sql" + sql + ":\n" + sqlToEsQuery(sql));
//
//        sql = "SELECT sum(num_d) as num2,split(newtype,',') as nt  from  twitter2 group by nt  order by num2 ";
//
//        System.out.println("sql" + sql + ":\n" + sqlToEsQuery(sql));
//
//        sql = "SELECT newtype as nt  from  twitter2  ";
//
//        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));
//
//        sql = "SELECT sum(num_d) as num2,floor(num) as nt  from  twitter2 group by floor(num),newtype  order by num2 ";
//
//        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));
//
//        sql = "SELECT split('newtype','b')[1] as nt,sum(num_d) as num2   from  twitter2 group by nt ";
//
//        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));
//
//        sql = "SELECT concat_ws('dd','newtype','num_d') as num2   from  twitter2";
//        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));
//
//        sql = "SELECT split(split('newtype','c')[0],'b')[0] as num2   from  twitter2";
//        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));
//
//        sql = "SELECT floor(split(substring('newtype',0,3),'c')[0]) as num2   from  twitter2";
//        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));
//
//        sql = "SELECT split(substring('newtype',0,3),'c')[0] as nt,num_d   from  twitter2 group by nt";
//        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));
//
//        sql = "SELECT floor(num_d) as nt from  twitter2 ";
//        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));
//
//        sql = "SELECT trim(newtype) as nt from  twitter2 ";
//        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));
//
//        sql = "SELECT trim(concat_ws('dd',newtype,num_d)) as nt from  twitter2 ";
//        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));
//
//
//        sql = "SELECT split(trim(concat_ws('dd',newtype,num_d)),'dd')[0] as nt from  twitter2 ";
//        System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));

        // sql = "SELECT floor(" +
        //         "floor(substring(newtype,0,14)/100)/5)*5 as key," +
        //         "count(distinct(num)) cvalue FROM twitter2 " +
        //         "group by key ";
        // String TEST_INDEX = "elasticsearch-sql_test_index";

        // sql =  "select * from xxx/locs where 'a' = 'b' and a > 1";

        // System.out.println("sql" + sql + ":\n----------\n" + sqlToEsQuery(sql));


        Properties properties = new Properties();
        properties.put(PROP_URL, "jdbc:elasticsearch://113.207.36.248:31010");
        properties.put(PROP_CONNECTIONPROPERTIES, "client.transport.ignore_cluster_name=true");
        DruidDataSource dds = (DruidDataSource) ElasticSearchDruidDataSourceFactory.createDataSource(properties);
        Connection connection = dds.getConnection();
        PreparedStatement ps = connection.prepareStatement("select name as org_name from ccp_org");

        // PreparedStatement ps = connection.prepareStatement("SELECT /*! USE_SCROLL*/ gender,lastname,age,_scroll_id from  " + TestsConstants.TEST_INDEX_ACCOUNT + " where lastname='Heath'");
        ResultSet resultSet = ps.executeQuery();

        // ResultSetMetaData metaData = resultSet.getMetaData();
        // assertThat(metaData.getColumnName(1), equalTo("gender"));
        // assertThat(metaData.getColumnName(2), equalTo("lastname"));
        // assertThat(metaData.getColumnName(3), equalTo("age"));

        List<String> result = new ArrayList<String>();
        String scrollId = null;
        while (resultSet.next()) {
            scrollId = resultSet.getString("org_name");
        }

        ps.close();
        connection.close();
        dds.close();

        // Assert.assertEquals(1, result.size());
        // Assert.assertEquals("Heath,39,F", result.get(0));
        // Assert.assertFalse(Matchers.isEmptyOrNullString().matches(scrollId));

    }
}
