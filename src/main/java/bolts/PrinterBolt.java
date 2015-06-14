package bolts;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import redis.clients.jedis.Jedis;

import java.io.StringWriter;
import java.sql.*;




import java.io.IOException;
import java.text.NumberFormat;
import java.util.Calendar;
import java.util.Locale;
import java.util.Map;
import com.datastax.driver.core.PreparedStatement;


public class PrinterBolt extends BaseRichBolt {

    private String name;
    private int ID;

    OutputCollector outputCollector;
    private String pname;
    private double pprice;
    private Connection conn;
    private Jedis jedis;
    PreparedStatement preparedStatement;
    Session session;
    JSONObject jsonObject = new JSONObject();

    @Override
    public void cleanup() {
        System.out.println("Bye Bye I'm "+name+" with id :"+ID);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void prepare(Map map, TopologyContext context, OutputCollector outputCollector) {
        this.name =  context.getThisComponentId();
        this.ID = context.getThisTaskId();
        this.outputCollector = outputCollector;
        this.jedis = new Jedis("localhost");
        try{
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            conn = DriverManager.getConnection("jdbc:sqlserver://b4m15im6ok.database.windows.net:1433;database=crawler;user=xxxx;password=xxxx;encrypt=true;hostNameInCertificate=*.database.windows.net;loginTimeout=30;");
            System.out.println("Connected");
        }
        catch (Exception e){
            e.printStackTrace();
        }

        Cluster cluster;

        cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
        session = cluster.connect("smartprice");
        String cql = "INSERT INTO products(product_id,month,crawl_time,price,url) VALUES (?,?,?,?,?)";
        preparedStatement = session.prepare(cql);

    }


    @Override
    public void execute(Tuple tuple) {
        String data = tuple.getString(0); // json string is returned
        String url = new String();
        String uid = new String();
        String site = new String();
        //String site = tuple.getString(1);
        Object object = null;
        try {
            object = new JSONParser().parse(data);
            jsonObject = (JSONObject) object;
            System.out.println(jsonObject.get("url"));
            System.out.println(jsonObject.get("site"));
            System.out.println(jsonObject.get("uid"));
            url = jsonObject.get("url").toString();
            uid = jsonObject.get("uid").toString();
            site = jsonObject.get("site").toString();
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println(data);
        }



        int flag = 0;


        Document doc = null;



        if(site.equals("amazon")){

            try {
                doc = Jsoup.connect(url).get();
                Elements mainCards = doc.select("#productTitle");
                Elements prices = doc.select("#olp_feature_div > div > span > span");
                for(Element card: mainCards) {
                    pname = card.text();
                    System.out.println("Product   :  " + pname);
                }
                for(Element price: prices){
                    String temp = price.text();
                    temp = temp.replaceAll("\u00A0", "");
                    temp = temp.replaceAll(" ", "");
                    NumberFormat format = NumberFormat.getInstance(Locale.ENGLISH);
                    try{
                        Number number = format.parse(temp);
                        pprice = number.doubleValue();
                        System.out.println("Price     :  "+pprice);


                        try{
                            String query = "insert into Products (uid, pname, pprice, site , url) values(?,?,?,?,?)";
                            Calendar cal = Calendar.getInstance();
                            String month = Integer.toString(cal.get(Calendar.MONTH));
                            BoundStatement boundStatement = preparedStatement.bind(uid,month,cal.getTime(),pprice,url);
                            session.execute(boundStatement);
                            java.sql.PreparedStatement preparedStatement = conn.prepareStatement(query);
                            preparedStatement.setString(1,uid);
                            preparedStatement.setString(2,pname);
                            preparedStatement.setDouble(3,pprice);
                            preparedStatement.setString(4,site);
                            preparedStatement.setString(5,url);

                            preparedStatement.executeUpdate();
                            jedis.lpush("finished.amazon",uid);
                            JSONObject jo = new JSONObject();
                            jo.put("pname",pname);
                            jo.put("pprice",pprice);
                            jo.put("site",site);
                            jo.put("uid",uid);
                            StringWriter out = new StringWriter();
                            jo.writeJSONString(out);
                            String jsonData = out.toString();
                            jedis.set("details."+uid,jsonData);

                        }
                        catch(Exception e){
                            System.out.println(e);
                        }

                    }
                    catch(Exception e){
                        System.out.println("Decimal Error ;)");
                        e.printStackTrace();
                    }


                }
            }
            catch (IOException e) {
                //e.printStackTrace();
                System.out.println(e.getMessage());
                this.outputCollector.fail(tuple);
                flag = 1;
            }


        }
        if(site.equals("flipkart")){


            try {
                doc = Jsoup.connect(url).get();
                System.out.println("reached here");
                Elements mainCards = doc.select("h1.title");
                Elements prices = doc.select("span.selling-price.omniture-field");
                for(Element card: mainCards) {
                    pname = card.text();
                    System.out.println("Product   :  " + pname);
                }
                for(Element price: prices){
//                    String temp = price.text();
                    String temp = price.attr("data-evar48");
//                    temp = temp.replaceAll("\u00A0", "");
//                    temp = temp.replaceAll(" ", "");
                    NumberFormat format = NumberFormat.getInstance(Locale.ENGLISH);
                    try{
                        pprice = Double.parseDouble(temp);
//                        Number number = format.parse(temp);
//                        pprice = number.doubleValue();
                        System.out.println("Price     :  "+pprice);


                        try{
                            String query = "insert into Products (uid, pname, pprice, site) values(?,?,?,?)";
                            Calendar cal = Calendar.getInstance();
                            String month = Integer.toString(cal.get(Calendar.MONTH));
                            BoundStatement boundStatement = preparedStatement.bind(uid,month,cal.getTime(),pprice,url);
                            session.execute(boundStatement);

                            java.sql.PreparedStatement preparedStatement = conn.prepareStatement(query);
                            preparedStatement.setString(1,uid);
                            preparedStatement.setString(2,pname);
                            preparedStatement.setDouble(3,pprice);
                            preparedStatement.setString(4,site);
                            preparedStatement.setString(5,url);

                            preparedStatement.executeUpdate();
                            jedis.lpush("finished.amazon",uid);
                            JSONObject jo = new JSONObject();
                            jo.put("pname",pname);
                            jo.put("pprice",pprice);
                            jo.put("site",site);
                            jo.put("uid",uid);
                            StringWriter out = new StringWriter();
                            jo.writeJSONString(out);
                            String jsonData = out.toString();
                            jedis.set("details."+uid,jsonData);

                        }
                        catch(Exception e){
                            System.out.println(e);
                        }

                    }
                    catch(Exception e){
                        System.out.println("Decimal Error ;)");
                        e.printStackTrace();
                    }


                }
            }
            catch (IOException e) {
                //e.printStackTrace();
                System.out.println(e.getMessage());
                this.outputCollector.fail(tuple);
                flag = 1;
            }


        }









        if(flag==0) {
            System.out.println("uid       :  "+ uid);
            System.out.println("site      :  " + site);
            System.out.println("\n\n");

            this.outputCollector.ack(tuple);
        }
        else{
            flag = 0;
        }
    }




















//    @Override
//    public void execute(Tuple tuple) {
//        String uid = tuple.getString(0);
//        String site = tuple.getString(1);
//        int flag = 0;
//
//
//        Document doc = null;
//        try {
//            doc = Jsoup.connect("http://www.amazon.in/dp/"+uid).get();
//            Elements mainCards = doc.select("#productTitle");
//            Elements prices = doc.select("#olp_feature_div > div > span > span");
//            for(Element card: mainCards) {
//                pname = card.text();
//                System.out.println("Product   :  " + pname);
//
//            }
//
//            for(Element price: prices){
//                String temp = price.text();
//                temp = temp.replaceAll("\u00A0", "");
//                temp = temp.replaceAll(" ", "");
//                NumberFormat format = NumberFormat.getInstance(Locale.ENGLISH);
//                try{
//                    Number number = format.parse(temp);
//                    pprice = number.doubleValue();
//
//                    System.out.println("Price     :  "+pprice);
//
//                    try{
//                        String query = "insert into Products (uid, pname, pprice, site) values(?,?,?,?)";
//                        PreparedStatement preparedStatement = conn.prepareStatement(query);
//                        preparedStatement.setString(1,uid);
//                        preparedStatement.setString(2,pname);
//                        preparedStatement.setDouble(3,pprice);
//                        preparedStatement.setString(4,site);
//                        preparedStatement.executeUpdate();
//                        jedis.lpush("finished.amazon",uid);
//                        JSONObject jo = new JSONObject();
//                        jo.put("pname",pname);
//                        jo.put("pprice",pprice);
//                        jo.put("site",site);
//                        jo.put("uid",uid);
//                        StringWriter out = new StringWriter();
//                        jo.writeJSONString(out);
//                        String jsonData = out.toString();
//                        jedis.set("details."+uid,jsonData);
//
//                    }
//                    catch(Exception e){
//                        System.out.println(e);
//                    }
//
//                }
//                catch(Exception e){
//                    System.out.println("Decimal Error ;)");
//                    e.printStackTrace();
//                }
//                //System.out.println(temp);
//
//            }
//        } catch (IOException e) {
//            //e.printStackTrace();
//            System.out.println(e.getMessage());
//            this.outputCollector.fail(tuple);
//            flag = 1;
//
//
//
//        }
////        Elements newsHeadlines = doc.select("#mp-itn b a");
////        for(Element newsHeadline: newsHeadlines) {
////            System.out.println(newsHeadline.attr("abs:href"));
////        }
//
//
//
//
//
//
//
//
//
//        if(flag==0) {
//            System.out.println("uid       :  "+ uid);
//            System.out.println("site      :  " + site);
//            System.out.println("\n\n");
//
//            this.outputCollector.ack(tuple);
//        }
//        else{
//            flag = 0;
//        }
//    }
}
