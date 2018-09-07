package com;


import org.apache.mina.core.service.IoAcceptor;
import org.apache.mina.core.service.IoHandler;
import org.apache.mina.core.session.IdleStatus;
import org.apache.mina.core.session.IoSession;
import org.apache.mina.filter.codec.ProtocolCodecFilter;
import org.apache.mina.filter.codec.textline.TextLineCodecFactory;
import org.apache.mina.transport.socket.nio.NioSocketAcceptor;
import redis.clients.jedis.Jedis;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.KeyEvent;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * 通过Socket向服务输出查询请求，通过一个简易按钮界面来控制合适开始查询
 */
public class DataGenerator {
    // 显示一个带按钮的面板，当点击按钮时，才开始进行查询请求，最好等Train数据输入结束后再按，
    // 不然准确率和召回率都会很惨淡。
    private static void createAndShowGUI(SimpleServer ss) {
        JFrame frame = new JFrame("ButtonDemo");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);

        ButtonDemo newContentPane = new ButtonDemo(ss);
        newContentPane.setOpaque(true); //content panes must be opaque
        frame.setContentPane(newContentPane);

        frame.pack();
        frame.setVisible(true);
    }

    public static void main(String[] args) throws Exception {
        SimpleServer ss = new SimpleServer();
        javax.swing.SwingUtilities.invokeLater(new Runnable() {
            public void run() {
                createAndShowGUI(ss);
            }
        });
        ss.start();
    }
}

class ButtonDemo extends JPanel implements ActionListener {
    protected JButton b1;
    private SimpleServer ss;

    public ButtonDemo(SimpleServer ss) {
        this.ss = ss;
        b1 = new JButton("Start Checking...");
        b1.setVerticalTextPosition(AbstractButton.CENTER);
        b1.setHorizontalTextPosition(AbstractButton.LEADING);
        b1.setMnemonic(KeyEvent.VK_D);
        b1.setActionCommand("disable");
        b1.addActionListener(this);
        add(b1);
    }

    public void actionPerformed(ActionEvent e) {
        if (ss.isChecking()) {
            ss.setChecking(false);
            this.b1.setText("Stopped");
        } else {
            ss.setChecking(true);
            this.b1.setText("Checking");
        }
    }
}

/**
 * 用 Mina写的简易通讯框架，用于传输查询请求。
 */
class SimpleServer implements IoHandler {
    private volatile boolean isChecking = false;
    private List<String> testData = new ArrayList<>();
    private Map<IoSession, Integer> indexMap = new HashMap<>();

    public void start() throws IOException {
        IoAcceptor acceptor = new NioSocketAcceptor();
        acceptor.getFilterChain().addLast("codec", new ProtocolCodecFilter(new TextLineCodecFactory(StandardCharsets.UTF_8)));
        acceptor.getSessionConfig().setIdleTime(IdleStatus.WRITER_IDLE, 3);
        acceptor.setHandler(this);
        acceptor.bind(new InetSocketAddress(40080));
        System.out.println("Server started...");
    }

    /**
     * 会话建立时，读取测试集数据。
     * @param ioSession
     * @throws Exception
     */
    @Override
    public void sessionCreated(IoSession ioSession) throws Exception {
        BufferedReader br = new BufferedReader(new FileReader("/Users/ruoyudai/Documents/workspace/ml-100k/ua.test"));
        String line = null;
        int collected = 0;
        while ((line = br.readLine()) != null) {
            this.testData.add("1\t" + line);
        }
        br.close();
        System.out.println("Collected test data:[" + this.testData.size() + "]");

        this.indexMap.put(ioSession, 0);
    }

    @Override
    public void sessionOpened(IoSession ioSession) throws Exception {
    }

    @Override
    public void sessionClosed(IoSession ioSession) throws Exception {
        this.indexMap.remove(ioSession);
    }

    /**
     * 当通道写的空闲时间超过设定值时，会激发此方法；
     * 即每个一个固定时间，向流服务发送指定数量的查询请求，
     * 并马上计算准确率。默认每隔3秒，每次5条，可自行改代码调整。
     *
     * @param ioSession
     * @param idleStatus
     * @throws Exception
     */
    @Override
    public void sessionIdle(IoSession ioSession, IdleStatus idleStatus) throws Exception {
        if (isChecking) {
            doCheck(ioSession);
        }
    }

    private void doCheck(IoSession ioSession) {
        int count = this.indexMap.get(ioSession);

        for (int i = 0; i < 5 && (i + count < this.testData.size()); i++) {
            ioSession.write(this.testData.get(i + count));
            count++;
        }
        this.indexMap.put(ioSession, count);

        Jedis redis = new Jedis();
        List<String> preds = redis.lrange("recommodation", 0, -1);

        Map<String, Integer> predMap = new HashMap<String, Integer>();
        for (String pred : preds) {
            String[] s = pred.split(":");
            String user = s[0];
            String item = s[1];
            predMap.put(user + ":" + item, 1);
        }

        int precision = 0;
        for (String col : this.testData.subList(0, count)) {
            String[] ratings = col.split("\t");
            String user = ratings[0];
            String item = ratings[1];
            if (predMap.containsKey(user + ":" + item)) {
                precision++;
            }
        }

        int predAll = count;
        int all = 90000 + predAll;

        System.out.println("Test Over.Precision=" + precision + "/" + predAll + "=[" + precision * 1.0 / predAll
                + "], recall=" + precision + "/" + 100000 + "=[" + precision * 1.0 / 100000 + "]");
    }

    public void setChecking(boolean checking) {
        isChecking = checking;
        System.out.println("Now Checking:" + isChecking);
    }

    public boolean isChecking() {
        return isChecking;
    }

    @Override
    public void exceptionCaught(IoSession ioSession, Throwable throwable) throws Exception {

    }

    @Override
    public void messageReceived(IoSession ioSession, Object o) throws Exception {

    }

    @Override
    public void messageSent(IoSession ioSession, Object o) throws Exception {

    }

    @Override
    public void inputClosed(IoSession ioSession) throws Exception {

    }
}