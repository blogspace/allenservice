package rpctest;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RPCServer {
    private static ExecutorService pool = Executors.newCachedThreadPool();

    public static void main(String[] args) {
        //执行远程服务
        runRPCServer(new ServerServiceImpl(),6060);
    }

    /**
     * 执行远程服务
     * @param service 服务端实现类
     */
    private static void runRPCServer(Object service,int port){

        ServerSocket server = null;
        try {
            server = new ServerSocket(port);
            System.out.println("服务端口：" + port + "启动...");
            while(true){
                final Socket accept = server.accept();
                System.out.println("收到请求");
                pool.submit(new Runnable() {
                    public void run() {
                        ObjectInputStream input = null;
                        ObjectOutputStream output = null;
                        try {

                            // 从监听的socket中获得输入流
                            input = new ObjectInputStream(accept.getInputStream());
                            String methodName = input.readUTF();
                            Class<?>[] parameterTypes = (Class<?>[]) input.readObject();
                            //客户端代理中发送的代理方法的参数Object[]
                            Object[] args = (Object[]) input.readObject();
                            // 从监听的socket中获得输出流
                            output = new ObjectOutputStream(accept.getOutputStream());
                            Method method = service.getClass().getMethod(methodName, parameterTypes);
                            Object back = method.invoke(service, args);
                            output.writeObject(back);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            }

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if(null != server){
                    server.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
