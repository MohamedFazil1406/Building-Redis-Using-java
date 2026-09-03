package test;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;

public class TestClient {

    public static void main(String[] args) throws Exception {

        test("dir");
        test("appendonly");
        test("appenddirname");
        test("appendfilename");
    }

    static void test(String parameter) throws Exception {

        Socket socket =
                new Socket("localhost", 6379);

        InputStream input =
                socket.getInputStream();

        OutputStream output =
                socket.getOutputStream();

        String command =
                "*3\r\n" +
                        "$6\r\n" +
                        "CONFIG\r\n" +
                        "$3\r\n" +
                        "GET\r\n" +
                        "$" + parameter.length() + "\r\n" +
                        parameter + "\r\n";

        output.write(
                command.getBytes(
                        StandardCharsets.UTF_8
                )
        );

        output.flush();

        byte[] buffer =
                new byte[1024];

        int n =
                input.read(buffer);

        System.out.println(
                "CONFIG GET " + parameter
        );

        System.out.println(
                new String(
                        buffer,
                        0,
                        n,
                        StandardCharsets.UTF_8
                )
        );

        socket.close();
    }
}