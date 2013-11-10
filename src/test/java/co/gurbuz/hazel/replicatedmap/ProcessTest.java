package co.gurbuz.hazel.replicatedmap;

import java.io.File;
import java.io.InputStream;

/**
 * @ali 10/11/13
 */
public class ProcessTest {

    public static void main(String[] args) throws Exception {

        for (int i=0; i<3; i++) {
            final int id = i;
            new Thread(){
                public void run() {
                    System.err.println("\n\n\t\t ---- start of process asdf counter: " + id + " \n\n");
                    try {
                        startProcess("co.gurbuz.hazel.replicatedmap.MainTest", "instance_" + id);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }.start();
            Thread.sleep(1000);
        }

    }

    public static int startProcess(String className, String arg) throws Exception {
        final String property = System.getProperty("java.class.path");

        String[] command = {"java","-cp", "\"" + property + "\"", className, arg};
        ProcessBuilder proBuilder = new ProcessBuilder( command ).redirectErrorStream(true);
        proBuilder.directory(new File("/java/workspace/ReplicatedMap/target/test-classes"));


        Process process = proBuilder.start();

        InputStream in = process.getInputStream();

        byte[] data = new byte[1024];

        int len = -1;

        while ((len = in.read(data)) != -1 ){
            System.err.print(new String(data, 0 , len));
        }

        return process.waitFor();
    }

}
