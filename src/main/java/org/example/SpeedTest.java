package org.example;

import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.exception.AuthFailedException;
import com.vesoft.nebula.client.graph.exception.ClientServerIncompatibleException;
import com.vesoft.nebula.client.graph.exception.IOErrorException;
import com.vesoft.nebula.client.graph.exception.NotValidConnectionException;
import com.vesoft.nebula.client.graph.net.Session;

import java.net.UnknownHostException;

public class SpeedTest {
    public static void speedTestMultipleClient(int numClient) throws UnknownHostException, IOErrorException, AuthFailedException, ClientServerIncompatibleException, NotValidConnectionException {
        long start = System.currentTimeMillis();
        for (int i = 0; i < numClient; i++) {
            NebulaClient nebulaClient = NebulaHelper.getNebulaClient(Config.NEBULA_HOST, Config.NEBULA_PORT);
            Session session = nebulaClient.getSession(Config.USERNAME, Config.PASSWORD, Config.RECONNECT);
            ResultSet resultSet = session.execute("SHOW HOSTS;");
            session.release();
            nebulaClient.close();
        }
        long end = System.currentTimeMillis();
        System.out.println("Multiple clients take: " + (end - start) + "ms");
    }

    public static void speedTestSingletonClient(int numClient) throws UnknownHostException, IOErrorException, AuthFailedException, ClientServerIncompatibleException, NotValidConnectionException {
        long start = System.currentTimeMillis();
        for (int i = 0; i < numClient; i++) {
            NebulaClient nebulaClient = NebulaHelper.getNebulaSingletonClient(Config.NEBULA_HOST, Config.NEBULA_PORT);
            Session session = nebulaClient.getSession(Config.USERNAME, Config.PASSWORD, Config.RECONNECT);
            ResultSet resultSet = session.execute("SHOW HOSTS;");
            session.release();
        }
        NebulaClient nebulaClient = NebulaHelper.getNebulaSingletonClient(Config.NEBULA_HOST, Config.NEBULA_PORT);
        nebulaClient.close();
        long end = System.currentTimeMillis();
        System.out.println("Singleton client takes: " + (end - start) + "ms");
    }


    public static void main(String[] args) throws InterruptedException {
        Thread t1 = new Thread(new Runnable() {
            public void run() {
                try {
                    speedTestMultipleClient(300);
                } catch (UnknownHostException | IOErrorException | AuthFailedException |
                         ClientServerIncompatibleException |
                         NotValidConnectionException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        Thread t2 = new Thread(new Runnable() {
            public void run() {
                try {
                    speedTestSingletonClient(300);
                } catch (UnknownHostException | IOErrorException | AuthFailedException |
                         ClientServerIncompatibleException |
                         NotValidConnectionException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        long start = System.currentTimeMillis();
        System.out.println("--- START ---");
        t1.start();
        t2.start();
        t1.join();
        t2.join();
        long end = System.currentTimeMillis();
        System.out.println("--- END --- (" + (end - start) + "ms)");

    }
}