import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {

    public static void main(String[] args) {

        System.out.println("Logs from your program will appear here!");

        int port = 6379;
        boolean isReplica = false;

        String masterHost = null;
        int masterPort = 0;

        String replicationId =
                "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";

        long replicationOffset = 0;

        // Read command-line arguments
        for (int i = 0; i < args.length; i++) {

            if (args[i].equals("--port")) {
                port = Integer.parseInt(args[i + 1]);
            }

            if (args[i].equals("--replicaof")) {
                isReplica = true;

                String[] masterInfo = args[i + 1].split(" ");

                masterHost = masterInfo[0];
                masterPort = Integer.parseInt(masterInfo[1]);
            }
        }

        // Connect to master if running as replica
        if (isReplica) {
            connectToMaster(masterHost, masterPort);
        }

        try (ServerSocket serverSocket = new ServerSocket(port)) {

            serverSocket.setReuseAddress(true);

            System.out.println(
                    "Redis server running on port " + port);

            CommandHandler handler =
                    new CommandHandler(
                            isReplica,
                            replicationId,
                            replicationOffset);

            while (true) {

                Socket client = serverSocket.accept();

                new Thread(
                        () -> handleClient(client, handler)
                ).start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    static void connectToMaster(String host, int port) {

        try {

            Socket master =
                    new Socket(host, port);

            System.out.println(
                    "Connected to master "
                            + host + ":" + port);

            String ping =
                    "*1\r\n$4\r\nPING\r\n";

            master.getOutputStream()
                    .write(ping.getBytes());

            master.getOutputStream().flush();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    static void handleClient(
            Socket client,
            CommandHandler handler) {

        try {

            InputStream inputStream =
                    client.getInputStream();

            byte[] buffer = new byte[1024];

            int bytesRead;

            while ((bytesRead =
                    inputStream.read(buffer)) != -1) {

                String request =
                        new String(
                                buffer,
                                0,
                                bytesRead);

                System.out.println(
                        "Received: " + request);

                client.getOutputStream()
                        .write("+PONG\r\n".getBytes());
            }

            client.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}