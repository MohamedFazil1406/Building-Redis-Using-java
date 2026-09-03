package test;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;

public class TestClient {

    public static void main(String[] args) throws Exception {

        test("SET", "foo", "bar");

        test("GET", "foo");

        test("PING");

        test("ECHO", "hello");

        test("SET", "bar", "baz");
    }

    static void test(String... tokens) throws Exception {

        Socket socket =
                new Socket("localhost", 6379);

        InputStream input =
                socket.getInputStream();

        OutputStream output =
                socket.getOutputStream();


        // Build RESP command
        StringBuilder command =
                new StringBuilder();

        command.append("*")
                .append(tokens.length)
                .append("\r\n");

        for (String token : tokens) {

            byte[] bytes =
                    token.getBytes(
                            StandardCharsets.UTF_8
                    );

            command.append("$")
                    .append(bytes.length)
                    .append("\r\n");

            command.append(token)
                    .append("\r\n");
        }


        // Send command
        output.write(
                command.toString()
                        .getBytes(StandardCharsets.UTF_8)
        );

        output.flush();


        // Read response
        byte[] buffer =
                new byte[1024];

        int n =
                input.read(buffer);


        System.out.println(
                "Command: " +
                        String.join(" ", tokens)
        );

        System.out.println(
                "Response: " +
                        new String(
                                buffer,
                                0,
                                n,
                                StandardCharsets.UTF_8
                        )
        );

        System.out.println("----------------");


        socket.close();
    }
}