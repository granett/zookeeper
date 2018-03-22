package zookeeper.zookeeper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.util.ZxidUtils;

public class ZookeeperTest {

    private static String zkip = "192.168.1.207:2181";

    private static ZooKeeper zk;

    static{
        try {
            zk = new ZooKeeper(zkip, 6000, new Watcher() {
                public void process(WatchedEvent watchedEvent) {
                    System.out.println("Receive event "+watchedEvent);
                    if(Event.KeeperState.SyncConnected == watchedEvent.getState())
                        System.out.println("connection is ok");
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void create() throws Exception{
    	zk.create("/zx", "aaa".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    	String value = new String(zk.getData("/zx", false, null));
    	System.out.println(value);
    }
    
    public static void update() throws Exception{
    	zk.create("/zx1", "aaa".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    	String value = new String(zk.getData("/zx1", false, null));
    	System.out.println(value);
    	zk.setData("/zx1", "bbb".getBytes(),-1);
    	value = new String(zk.getData("/zx1", false, null));
    	System.out.println(value);
    }
    
    public static void delete() throws Exception{
    	zk.create("/zx2", "aaa".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    	String value = new String(zk.getData("/zx2", false, null));
    	System.out.println(value);
    	zk.delete("/zx2",0);
    	value = new String(zk.getData("/zx2", false, null));
    	System.out.println(value);
    }
    
    public static void watch() throws Exception{
    	zk.create("/zx3", "aaa".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    	zk.exists("/zx3", new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println(111);
                System.out.println(watchedEvent.getPath());
                System.out.println(watchedEvent.getState());
                System.out.println(watchedEvent.getType());
            }
        });
    	zk.delete("/zx3", -1);
    	System.out.println(222);
    }
    
    public static void AsyncCallback() throws Exception{
    	zk.create("/zx4", "aaa".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    	zk.getData("/zx4", false, new AsyncCallback.DataCallback() {
            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
        		System.out.println(2);
        		System.out.println(path);
        		System.out.println(new String(data));
            }
        }, null);
    	Thread.sleep(100000);
    }




    
    public static void main(String args[]) throws Exception{
    	update();
//    	delete();
//    	watch();
//    	AsyncCallback();
    }
   
    
    
    /**
     * 获取锁
     * @return
     * @throws InterruptedException
     */
    public void lock(){}

    /**
     * 释放锁
     */
    public void unlock(){}

}
