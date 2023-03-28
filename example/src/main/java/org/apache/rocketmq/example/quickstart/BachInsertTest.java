package org.apache.rocketmq.example.quickstart;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @description:
 * @author: xiaochangbai
 * @date: 2023/3/28 17:47
 */
public class BachInsertTest {

    public static final int MESSAGE_COUNT = 1000;
    public static final String PRODUCER_GROUP = "testGroup1";
    public static final String DEFAULT_NAMESRVADDR = "127.0.0.1:9876";
    public static final String TOPIC = "t3";
    public static final String TAG = "TagAa";


    public static void main(String[] args) throws MQClientException {
        AtomicLong totalCount = new AtomicLong(0L);
        AtomicLong successCount = new AtomicLong(0L);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(50, 50, 10,
                TimeUnit.MINUTES, new ArrayBlockingQueue<>(10000), new ThreadFactory() {
            AtomicLong atomicLong = new AtomicLong(0L);
            @Override
            public Thread newThread(Runnable r) {
                return new Thread(r,"并发生成id测试-"+atomicLong.incrementAndGet());
            }
        },new ThreadPoolExecutor.DiscardPolicy());

        DefaultMQProducer defaultMQProducer = buildSender();
        long startTime = System.currentTimeMillis();
        long executorTime = 1000*10;
        while (System.currentTimeMillis()-startTime<executorTime){
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    totalCount.incrementAndGet();
                    if(sendMsg(defaultMQProducer,"testC","testC", UUID.randomUUID().toString())){
                        successCount.incrementAndGet();
                    }
                }
            });
        }
        System.out.printf("测试完成，耗时：%s/s,消息总量:%s,成功率;%s",(System.currentTimeMillis()-startTime)/1000,
                totalCount.get(),(successCount.get()*1.0/totalCount.get())*100);
        executor.shutdown();
    }


    public static DefaultMQProducer buildSender() throws MQClientException {
        /*
         * Instantiate with a producer group name.
         */
        DefaultMQProducer producer = new DefaultMQProducer(PRODUCER_GROUP,true);

        /*
         * Specify name server addresses.
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         *  producer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */
        // Uncomment the following line while debugging, namesrvAddr should be set to your local address
        producer.setNamesrvAddr(DEFAULT_NAMESRVADDR);


        /*
         * Launch the instance.
         */
        producer.start();

        return producer;
    }


    public static boolean sendMsg(DefaultMQProducer producer,
                                  String topic,String tags,String context){
        Message msg = null;
        try {
            msg = new Message(topic /* Topic */,
                    tags /* Tag */,
                    context.getBytes(RemotingHelper.DEFAULT_CHARSET) /* Message body */
            );
            SendResult sendResult = producer.send(msg);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
