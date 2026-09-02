import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class Main {

    // Empty RDB file
    // Replace this Base64 string with the empty RDB Base64
    // provided by the CodeCrafters challenge.
    static final byte[] EMPTY_RDB =
            Base64.getDecoder().decode(
                    "UkVESVMwMDEwOAAAAAAAAA=="
            );


    public static void main(String[] args) {

        System.out.println(
                "Logs from your program will appear here!"
        );

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

                String[] masterInfo =
                        args[i + 1].split(" ");

                masterHost = masterInfo[0];

                masterPort =
                        Integer.parseInt(masterInfo[1]);
            }
        }


        // Start server first
        try (ServerSocket serverSocket =
                     new ServerSocket(port)) {

            serverSocket.setReuseAddress(true);

            System.out.println(
                    "Redis server running on port "
                            + port
            );

            CommandHandler handler =
                    new CommandHandler(
                            isReplica,
                            replicationId,
                            replicationOffset
                    );


            // Connect to master in another thread
            if (isReplica) {

                String finalMasterHost = masterHost;
                int finalMasterPort = masterPort;
                int finalReplicaPort = port;

                new Thread(() ->
                        connectToMaster(
                                finalMasterHost,
                                finalMasterPort,
                                finalReplicaPort
                        )
                ).start();
            }


            // Accept clients
            while (true) {

                Socket client =
                        serverSocket.accept();

                new Thread(() ->
                        handleClient(
                                client,
                                handler
                        )
                ).start();
            }

        } catch (IOException e) {

            e.printStackTrace();
        }
    }


    // =====================================================
    // REPLICA -> MASTER
    // =====================================================

    static void connectToMaster(
            String host,
            int masterPort,
            int replicaPort
    ) {

        try {

            Socket master =
                    new Socket(
                            host,
                            masterPort
                    );

            System.out.println(
                    "Connected to master "
                            + host
                            + ":"
                            + masterPort
            );

            InputStream input =
                    master.getInputStream();

            OutputStream output =
                    master.getOutputStream();


            // =================================================
            // 1. PING
            // =================================================

            String ping =
                    "*1\r\n" +
                            "$4\r\n" +
                            "PING\r\n";

            output.write(
                    ping.getBytes(
                            StandardCharsets.UTF_8
                    )
            );

            output.flush();

            System.out.println(
                    "Master: "
                            + readResponse(input)
            );


            // =================================================
            // 2. REPLCONF listening-port
            // =================================================

            String portString =
                    String.valueOf(replicaPort);

            String replconfPort =
                    "*3\r\n" +
                            "$8\r\n" +
                            "REPLCONF\r\n" +
                            "$14\r\n" +
                            "listening-port\r\n" +
                            "$" +
                            portString.length() +
                            "\r\n" +
                            portString +
                            "\r\n";

            output.write(
                    replconfPort.getBytes(
                            StandardCharsets.UTF_8
                    )
            );

            output.flush();

            System.out.println(
                    "Master: "
                            + readResponse(input)
            );


            // =================================================
            // 3. REPLCONF capa psync2
            // =================================================

            String replconfCapa =
                    "*3\r\n" +
                            "$8\r\n" +
                            "REPLCONF\r\n" +
                            "$4\r\n" +
                            "capa\r\n" +
                            "$6\r\n" +
                            "psync2\r\n";

            output.write(
                    replconfCapa.getBytes(
                            StandardCharsets.UTF_8
                    )
            );

            output.flush();

            System.out.println(
                    "Master: "
                            + readResponse(input)
            );


            // =================================================
            // 4. PSYNC ? -1
            // =================================================

            String psync =
                    "*3\r\n" +
                            "$5\r\n" +
                            "PSYNC\r\n" +
                            "$1\r\n" +
                            "?\r\n" +
                            "$2\r\n" +
                            "-1\r\n";

            output.write(
                    psync.getBytes(
                            StandardCharsets.UTF_8
                    )
            );

            output.flush();


            // Read FULLRESYNC
            System.out.println(
                    "Master: "
                            + readResponse(input)
            );


            // =================================================
            // 5. Read RDB
            // =================================================

            int firstByte =
                    input.read();

            if (firstByte == '$') {

                String lengthString =
                        readLine(input);

                int rdbLength =
                        Integer.parseInt(lengthString);

                byte[] rdb =
                        input.readNBytes(rdbLength);

                System.out.println(
                        "Received RDB: "
                                + rdb.length
                                + " bytes"
                );
            }

            master.close();

        } catch (IOException e) {

            System.out.println(
                    "Replication error: "
                            + e.getMessage()
            );
        }
    }


    // =====================================================
    // MASTER CLIENT HANDLER
    // =====================================================

    static void handleClient(
            Socket client,
            CommandHandler handler
    ) {

        try {

            InputStream input =
                    client.getInputStream();

            OutputStream output =
                    client.getOutputStream();


            while (true) {

                String[] tokens =
                        parseRESP(input);

                if (tokens == null) {
                    break;
                }


                System.out.println(
                        "Received: "
                                + String.join(
                                " ",
                                tokens
                        )
                );


                String response =
                        handler.handle(tokens);


                System.out.println(
                        "Response: ["
                                + response
                                + "]"
                );


                // Send normal command response
                output.write(
                        response.getBytes(
                                StandardCharsets.UTF_8
                        )
                );

                output.flush();


                // =================================================
                // PSYNC -> send RDB
                // =================================================

                if (tokens.length > 0 &&
                        tokens[0].equalsIgnoreCase("PSYNC")) {

                    byte[] rdb =
                            EMPTY_RDB;


                    String header =
                            "$" +
                                    rdb.length +
                                    "\r\n";


                    output.write(
                            header.getBytes(
                                    StandardCharsets.UTF_8
                            )
                    );


                    // IMPORTANT:
                    // Send binary bytes directly
                    output.write(rdb);

                    output.flush();


                    System.out.println(
                            "Sent empty RDB: "
                                    + rdb.length
                                    + " bytes"
                    );
                }
            }

            client.close();

        } catch (IOException e) {

            e.printStackTrace();
        }
    }


    // =====================================================
    // RESP PARSER
    // =====================================================

    static String[] parseRESP(
            InputStream input
    ) throws IOException {

        int firstByte =
                input.read();

        if (firstByte == -1) {
            return null;
        }

        if (firstByte != '*') {
            return null;
        }


        int numberOfArguments =
                Integer.parseInt(
                        readLine(input)
                );


        String[] tokens =
                new String[numberOfArguments];


        for (int i = 0;
             i < numberOfArguments;
             i++) {

            int type =
                    input.read();

            if (type != '$') {
                return null;
            }


            int length =
                    Integer.parseInt(
                            readLine(input)
                    );


            byte[] data =
                    input.readNBytes(length);


            // Consume \r\n
            input.read();
            input.read();


            tokens[i] =
                    new String(
                            data,
                            StandardCharsets.UTF_8
                    );
        }


        return tokens;
    }


    // =====================================================
    // READ LINE
    // =====================================================

    static String readLine(
            InputStream input
    ) throws IOException {

        StringBuilder result =
                new StringBuilder();


        int previous = -1;


        while (true) {

            int current =
                    input.read();


            if (current == -1) {
                throw new IOException(
                        "Connection closed"
                );
            }


            if (previous == '\r' &&
                    current == '\n') {

                result.setLength(
                        result.length() - 1
                );

                return result.toString();
            }


            result.append(
                    (char) current
            );

            previous = current;
        }
    }


    // =====================================================
    // READ MASTER RESPONSE
    // =====================================================

    static String readResponse(
            InputStream input
    ) throws IOException {

        StringBuilder response =
                new StringBuilder();


        int first =
                input.read();


        if (first == -1) {
            return "EOF";
        }


        response.append(
                (char) first
        );


        int ch;

        while ((ch = input.read()) != -1) {

            response.append(
                    (char) ch
            );

            if (ch == '\n') {
                break;
            }
        }


        return response.toString();
    }
}