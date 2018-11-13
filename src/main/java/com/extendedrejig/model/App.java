package com.extendedrejig.model;
import java.util.HashMap;
import java.util.Map;

import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;

public class App {
  public static void main(String[] args) {

    String server1 = "40.112.132.76:11211";
    String[] serverList1 = new String[] {server1};
    String server2 = "40.112.132.76:11212";
    String[] serverList2 = new String[] {server2};
    String server3 = "40.112.132.76:11213";
    String[] serverList3 = new String[] {server3};
    String server4 = "40.112.132.76:11214";
    String[] serverList4 = new String[] {server4};

    Map<String, String> configuration = new HashMap<String, String>();
    configuration.put("1", server1); // <Fragment Number, IP:Port>
    configuration.put("2", server1); // <Fragment Number, IP:Port>
    configuration.put("3", server2); // <Fragment Number, IP:Port>
    configuration.put("4", server2); // <Fragment Number, IP:Port>
    configuration.put("5", server3); // <Fragment Number, IP:Port>
    configuration.put("6", server3); // <Fragment Number, IP:Port>
    configuration.put("7", server4); // <Fragment Number, IP:Port>
    configuration.put("8", server4); // <Fragment Number, IP:Port>








    SockIOPool pool1 = SockIOPool.getInstance( server1 );
    pool1.setServers(serverList1);
    SockIOPool pool2 = SockIOPool.getInstance( server2 );
    pool2.setServers(serverList2);
    SockIOPool pool3 = SockIOPool.getInstance( server3 );
    pool3.setServers(serverList3);
    SockIOPool pool4 = SockIOPool.getInstance( server4 );
    pool4.setServers(serverList4);

    pool1.setInitConn( 100 );
    pool1.setMinConn( 100 );
    pool1.setMaxConn( 500 );
    pool1.setMaintSleep( 20 );
    pool1.setNagle( false );
    pool1.initialize();

    pool2.setInitConn( 100 );
    pool2.setMinConn( 100 );
    pool2.setMaxConn( 500 );
    pool2.setMaintSleep( 20 );
    pool2.setNagle( false );
    pool2.initialize();

    pool3.setInitConn( 100 );
    pool3.setMinConn( 100 );
    pool3.setMaxConn( 500 );
    pool3.setMaintSleep( 20 );
    pool3.setNagle( false );
    pool3.initialize();

    pool4.setInitConn( 100 );
    pool4.setMinConn( 100 );
    pool4.setMaxConn( 500 );
    pool4.setMaintSleep( 20 );
    pool4.setNagle( false );
    pool4.initialize();

    MemCachedClient mc1 = new MemCachedClient( server1 );
    MemCachedClient mc2 = new MemCachedClient( server2 );
    MemCachedClient mc3 = new MemCachedClient( server3 );
    MemCachedClient mc4 = new MemCachedClient( server4 );

    String[] keys = {"2000000","2000001","2000002","2000003"};
    String[] values = {"This is a test of a value 1", "This is a test of a value 2","This is a test of a value 3","This is a test of a value 4"};

    ////////////////////////////////////////////////////////////
    for (int i = 0; i < keys.length; i++) {
      if (Integer.parseInt(keys[i]) % 4 == 0) {
        mc1.set(keys[i], values[i]);
      }
      else if (Integer.parseInt(keys[i]) % 4 == 1) {
        mc2.set(keys[i], values[i]);
      }
      else if (Integer.parseInt(keys[i]) % 4 == 2) {
        mc3.set(keys[i], values[i]);
      }
      else {
        mc4.set(keys[i], values[i]);
      }
    }
    ////////////////////////////////////////////////////////////

    String output1 = (String) mc1.get(keys[0]);
    System.out.println("Output of Get from CMI1: " +  output1);
    String output2 = (String) mc2.get(keys[1]);
    System.out.println("Output of Get from CMI2: " +  output2);
    String output3 = (String) mc3.get(keys[2]);
    System.out.println("Output of Get from CMI3: " +  output3);
    String output4 = (String) mc4.get(keys[3]);
    System.out.println("Output of Get from CMI4: " +  output4);

    ///////////////////////////////////////////////////////////





    SockIOPool.getInstance( server1 ).shutDown();
    SockIOPool.getInstance( server2 ).shutDown();
    SockIOPool.getInstance( server3 ).shutDown();
    SockIOPool.getInstance( server4 ).shutDown();
  }
}

