package demo;


import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.annotation.Annotation;
import java.lang.reflect.*;

public class SparkDemo {

    public static void main(String[] args) {

        //不合法的构造函数
        //编译时错误：构造函数 SingleObject() 是不可见的
//        SingleObject object = new SingleObject();
        //获取唯一可用的对象
//        SingleObject object = SingleObject.getInstance();

        //显示消息
//        object.showMessage();
        final String ls = executeLinuxCmd("dir");
        System.out.println(ls);

    }
    public static String executeLinuxCmd(String cmd) {
        System.out.println("got cmd job : " + cmd);
        Runtime run = Runtime.getRuntime();
        try {
            Process process = run.exec(cmd);
            InputStream in = process.getInputStream();
            BufferedReader bs = new BufferedReader(new InputStreamReader(in));
            // System.out.println("[check] now size \n"+bs.readLine());
            StringBuffer out = new StringBuffer();
            byte[] b = new byte[8192];
            for (int n; (n = in.read(b)) != -1;) {
                out.append(new String(b, 0, n));
            }
            System.out.println("job result [" + out.toString() + "]");
            in.close();
            // process.waitFor();
            process.destroy();
            return out.toString();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
