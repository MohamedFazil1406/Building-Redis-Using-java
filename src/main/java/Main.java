import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;

public class Main {

    public static void main(String[] args) {

        System.out.println("Logs from your program will appear here!");

        int port = 6379;
        boolean isReplica = false;

        // Read command-line arguments
        for (int i = 0; i < args.length; i++) {

            if (args[i].equals("--port")) {
                port = Integer.parseInt(args[i + 1]);
            }

            if (args[i].equals("--replicaof")) {
                isReplica = true;
            }
        }

        try (ServerSocket serverSocket = new ServerSocket(port)) {

            serverSocket.setReuseAddress(true);

            System.out.println("Redis server running on port " + port);

            // Pass replica information to CommandHandler
            CommandHandler handler = new CommandHandler(isReplica);

            while (true) {

                Socket client = serverSocket.accept();

                new Thread(() -> handleClient(client, handler)).start();
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void handleClient(Socket client, CommandHandler handler) {

        try {

            InputStream inputStream = client.getInputStream();

            byte[] buffer = new byte[1024];

            int bytesRead;

            while ((bytesRead = inputStream.read(buffer)) != -1) {

                String request =
                        new String(buffer, 0, bytesRead);

                System.out.println("Received: " + request);

                // Temporary response
                client.getOutputStream()
                        .write("+PONG\r\n".getBytes());
            }

            client.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}