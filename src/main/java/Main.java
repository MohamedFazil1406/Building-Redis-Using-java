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
            connectToMaster(masterHost, masterPort,port);
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


    static void connectToMaster(String host, int masterPort, int replicaPort) {

        try {

            Socket master =
                    new Socket(host, masterPort);

            System.out.println(
                    "Connected to master "
                            + host + ":" + masterPort);

            // 1. PING
            String ping =
                    "*1\r\n$4\r\nPING\r\n";

            master.getOutputStream()
                    .write(ping.getBytes());

            master.getOutputStream().flush();

            System.out.println("Master: " + readResponse(master));



            // 2. REPLCONF listening-port
            String portString = String.valueOf(replicaPort);

            String replconfPort =
                    "*3\r\n" +
                            "$8\r\nREPLCONF\r\n" +
                            "$14\r\nlistening-port\r\n" +
                            "$" + portString.length() + "\r\n" +
                            portString + "\r\n";

            master.getOutputStream()
                    .write(replconfPort.getBytes());

            master.getOutputStream().flush();

            System.out.println("Master: " + readResponse(master));


            // 3. REPLCONF capa psync2
            String replconfCapa =
                    "*3\r\n" +
                            "$8\r\nREPLCONF\r\n" +
                            "$4\r\ncapa\r\n" +
                            "$6\r\npsync2\r\n";

            master.getOutputStream()
                    .write(replconfCapa.getBytes());

            master.getOutputStream().flush();

            System.out.println("Master: " + readResponse(master));


            // 4. PSYNC
            String psync =
                    "*3\r\n" +
                            "$5\r\nPSYNC\r\n" +
                            "$1\r\n?\r\n" +
                            "$2\r\n-1\r\n";

            master.getOutputStream()
                    .write(psync.getBytes());

            master.getOutputStream().flush();

            System.out.println("Master: " + readResponse(master));

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

                String[] tokens = parseRESP(request);

                String response = handler.handle(tokens);

                client.getOutputStream()
                        .write(response.getBytes());
            }

            client.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static String[] parseRESP(String request) {

        String[] lines = request.split("\r\n");

        int numberOfArguments = Integer.parseInt(
                lines[0].substring(1)
        );

        String[] tokens = new String[numberOfArguments];

        int tokenIndex = 0;

        for (int i = 1; i < lines.length && tokenIndex < numberOfArguments; i++) {

            if (lines[i].startsWith("$")) {

                i++; // move to the actual value

                tokens[tokenIndex] = lines[i];

                tokenIndex++;
            }
        }

        return tokens;
    }

    static String readResponse(Socket socket) throws IOException {

        InputStream input = socket.getInputStream();

        byte[] buffer = new byte[1024];

        int n = input.read(buffer);

        return new String(buffer, 0, n);
    }
}