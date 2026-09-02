package test;
import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;

public class TestClient {

    public static void main(String[] args) throws Exception {

        Socket socket = new Socket("localhost", 6379);

        InputStream input = socket.getInputStream();
        OutputStream output = socket.getOutputStream();

        String command =
                "*3\r\n" +
                        "$6\r\n" +
                        "CONFIG\r\n" +
                        "$3\r\n" +
                        "GET\r\n" +
                        "$3\r\n" +
                        "dir\r\n";

        output.write(
                command.getBytes(StandardCharsets.UTF_8)
        );

        output.flush();

        byte[] buffer = new byte[1024];

        int n = input.read(buffer);

        System.out.println(
                new String(buffer, 0, n)
        );

        socket.close();
    }
}