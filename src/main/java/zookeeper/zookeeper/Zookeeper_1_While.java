package zookeeper.zookeeper;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event;

/**
 * Zookeepr实现分布式锁
 */
public class Zookeeper_1_While {
	private String zkconfig = "192.168.1.207:2181";

	private static String lockNameSpace = "/mylock";

	private String nodeString = lockNameSpace + "/lock1";

	private static ZooKeeper zk;

	public Zookeeper_1_While() {
		try {
			zk = new ZooKeeper(zkconfig, 6000, new Watcher() {
				@Override
				public void process(WatchedEvent watchedEvent) {
					System.out.println("Receive event " + watchedEvent);
					if (Event.KeeperState.SyncConnected == watchedEvent.getState()) {
						System.out.println("connection is ok");
					}
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private void watchNode(String nodeString) throws InterruptedException {
		try {
			zk.exists(nodeString, new Watcher() {
				@Override
				public void process(WatchedEvent watchedEvent) {
					System.out.println("事件来了：" + watchedEvent.toString());
					if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
						System.out.println("delete事件");
					}
				}
			});
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 获取锁
	 * 
	 * @return
	 * @throws InterruptedException
	 */
	public boolean lock() throws InterruptedException {
		String path = null;
		watchNode(nodeString);
		while (true) {
			try {
				path = zk.create(nodeString, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
			} catch (KeeperException e) {
				System.out.println(Thread.currentThread().getName() + "请求失败");
				try {
					Thread.sleep(500);
				} catch (InterruptedException ex) {
					System.out.println("thread is notify");
				}
			}
			 if (path!=null&&!path.isEmpty()) {
				 System.out.println(Thread.currentThread().getName() + " 拿到 Lock...");
				 return true;
			 }
		}
	}

	/**
	 * 释放锁
	 */
	public void unlock() {
		try {
			zk.delete(nodeString, -1);
			System.out.println(Thread.currentThread().getName() +  "释放 Lock...");
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
	}

	public static void main(String args[]) throws InterruptedException {
		Zookeeper_1_While test = new Zookeeper_1_While();
		try {
			Thread.sleep(100);
			zk.create(lockNameSpace, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
		} catch (InterruptedException e) {
		}
		ExecutorService service = Executors.newFixedThreadPool(10);
		for (int i = 0; i < 4; i++) {
			service.execute(() -> {
				try {
					test.lock();
					Thread.sleep(1800);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				test.unlock();
			});
		}
		service.shutdown();
	}
}
