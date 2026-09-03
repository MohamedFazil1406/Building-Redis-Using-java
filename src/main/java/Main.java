import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    // Empty RDB
    static final byte[] EMPTY_RDB =
            Base64.getDecoder().decode(
                    "UkVESVMwMDEwOAAAAAAAAA=="
            );



    static AtomicInteger connectedReplicas =
            new AtomicInteger(0);

    static String dir = System.getProperty("user.dir");
    static String dbfilename = "dump.rdb";
    static boolean isReplicaConnection = false;

    static class ParsedCommand {
        String[] tokens;
        long bytes;

        ParsedCommand(String[] tokens, long bytes) {
            this.tokens = tokens;
            this.bytes = bytes;
        }
    }


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


        // ============================================
        // COMMAND LINE ARGUMENTS
        // ============================================

        for (int i = 0; i < args.length; i++) {

            if (args[i].equals("--port")) {
                port = Integer.parseInt(args[i + 1]);
            }

            if (args[i].equals("--dir")) {
                dir = args[i + 1];
            }

            if (args[i].equals("--dbfilename")) {
                dbfilename = args[i + 1];
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
                            replicationOffset,
                            connectedReplicas,
                            dir,
                            dbfilename
                    );


            // ============================================
            // START REPLICATION
            // ============================================

            if (isReplica) {

                String finalMasterHost = masterHost;
                int finalMasterPort = masterPort;
                int finalReplicaPort = port;

                new Thread(() ->
                        connectToMaster(
                                finalMasterHost,
                                finalMasterPort,
                                finalReplicaPort,
                                handler
                        )
                ).start();
            }


            // ============================================
            // ACCEPT CLIENT CONNECTIONS
            // ============================================

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


    // ====================================================
    // REPLICA -> MASTER
    // ====================================================

    static void connectToMaster(
            String host,
            int masterPort,
            int replicaPort,
            CommandHandler handler
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


            // ============================================
            // 1. PING
            // ============================================

            String ping =
                    "*1\r\n" +
                            "$4\r\n" +
                            "PING\r\n";

            send(output, ping);

            System.out.println(
                    "Master: "
                            + readResponse(input)
            );


            // ============================================
            // 2. REPLCONF listening-port
            // ============================================

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

            send(output, replconfPort);

            System.out.println(
                    "Master: "
                            + readResponse(input)
            );


            // ============================================
            // 3. REPLCONF capa psync2
            // ============================================

            String replconfCapa =
                    "*3\r\n" +
                            "$8\r\n" +
                            "REPLCONF\r\n" +
                            "$4\r\n" +
                            "capa\r\n" +
                            "$6\r\n" +
                            "psync2\r\n";

            send(output, replconfCapa);

            System.out.println(
                    "Master: "
                            + readResponse(input)
            );


            // ============================================
            // 4. PSYNC ? -1
            // ============================================

            String psync =
                    "*3\r\n" +
                            "$5\r\n" +
                            "PSYNC\r\n" +
                            "$1\r\n" +
                            "?\r\n" +
                            "$2\r\n" +
                            "-1\r\n";

            send(output, psync);


            // FULLRESYNC
            System.out.println(
                    "Master: "
                            + readResponse(input)
            );

            String command =
                    "*3\r\n" +
                            "$8\r\n" +
                            "REPLCONF\r\n" +
                            "$6\r\n" +
                            "GETACK\r\n" +
                            "$1\r\n" +
                            "*\r\n";

            send(output,command);

            System.out.println(
                    "Master: "
                            + readResponse(input)
            );


            // ============================================
            // 5. READ RDB
            // ============================================

            int firstByte =
                    input.read();

            if (firstByte == '$') {

                String lengthString =
                        readLine(input);

                int rdbLength =
                        Integer.parseInt(lengthString);

                byte[] rdb =
                        readExactly(
                                input,
                                rdbLength
                        );

                System.out.println(
                        "Received RDB: "
                                + rdb.length
                                + " bytes"
                );
            }


            // ============================================
            // 6. KEEP CONNECTION OPEN
            // ============================================

            // IMPORTANT:
            // Do NOT close master here.
            //
            // The master will continue sending commands
            // through this same connection.


            handleReplicationStream(
                    master,
                    handler
            );


        } catch (IOException e) {

            System.out.println(
                    "Replication error: "
                            + e.getMessage()
            );
        }
    }


    // ====================================================
    // PROCESS COMMANDS FROM MASTER
    // ====================================================

    static void handleReplicationStream(
            Socket master,
            CommandHandler handler
    ) throws IOException {

        InputStream input =
                master.getInputStream();

        OutputStream output =
                master.getOutputStream();


        long replicationOffset = 0;


        while (true) {

            ParsedCommand command =
                    parseRESPWithLength(input);


            if (command == null) {
                break;
            }


            String[] tokens =
                    command.tokens;


            System.out.println(
                    "Master command: "
                            + String.join(
                            " ",
                            tokens
                    )
            );


            // ============================================
            // GETACK
            // ============================================

            if (tokens.length >= 2
                    && tokens[0].equalsIgnoreCase("REPLCONF")
                    && tokens[1].equalsIgnoreCase("GETACK")) {


                // IMPORTANT:
                // Send current offset BEFORE adding
                // the GETACK command itself.

                String ack =
                        "*3\r\n" +
                                "$8\r\n" +
                                "REPLCONF\r\n" +
                                "$3\r\n" +
                                "ACK\r\n" +
                                "$" +
                                String.valueOf(
                                        String.valueOf(
                                                replicationOffset
                                        ).length()
                                ) +
                                "\r\n" +
                                replicationOffset +
                                "\r\n";


                send(output, ack);


                System.out.println(
                        "Sent: REPLCONF ACK "
                                + replicationOffset
                );


                // Now count GETACK itself
                replicationOffset +=
                        command.bytes;


                System.out.println(
                        "Offset: "
                                + replicationOffset
                );

                continue;
            }


            // ============================================
            // NORMAL MASTER COMMAND
            // ============================================

            handler.handle(tokens);


            // Count the entire RESP command
            replicationOffset +=
                    command.bytes;


            System.out.println(
                    "Offset: "
                            + replicationOffset
            );
        }
    }


    // ====================================================
    // NORMAL CLIENT
    // ====================================================

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

                ParsedCommand command =
                        parseRESPWithLength(input);


                if (command == null) {
                    break;
                }


                String[] tokens =
                        command.tokens;


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


                output.write(
                        response.getBytes(
                                StandardCharsets.UTF_8
                        )
                );

                output.flush();


                // ========================================
                // PSYNC -> SEND RDB
                // ========================================

                if (tokens.length > 0
                        && tokens[0].equalsIgnoreCase("PSYNC")) {

                    connectedReplicas.incrementAndGet();

                    System.out.println(
                            "PSYNC received"
                    );

                    System.out.println(
                            "Connected replicas: "
                                    + connectedReplicas.get()
                    );
                    byte[] rdb =
                            EMPTY_RDB;


                    String header =
                            "$"
                                    + rdb.length
                                    + "\r\n";


                    output.write(
                            header.getBytes(
                                    StandardCharsets.UTF_8
                            )
                    );


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

            System.out.println(
                    "Client error: "
                            + e.getMessage()
            );
        }
    }


    // ====================================================
    // RESP PARSER WITH BYTE COUNT
    // ====================================================

    static ParsedCommand parseRESPWithLength(
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


        long bytes =
                1;


        // Read number of arguments
        String countLine =
                readLineCounting(
                        input
                );


        bytes +=
                countLine.length()
                        + 2;


        int numberOfArguments =
                Integer.parseInt(
                        countLine
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


            bytes++;


            String lengthLine =
                    readLineCounting(
                            input
                    );


            bytes +=
                    lengthLine.length()
                            + 2;


            int length =
                    Integer.parseInt(
                            lengthLine
                    );


            byte[] data =
                    readExactly(
                            input,
                            length
                    );


            bytes += length;


            // Consume \r\n after value
            input.read();
            input.read();


            bytes += 2;


            tokens[i] =
                    new String(
                            data,
                            StandardCharsets.UTF_8
                    );
        }


        return new ParsedCommand(
                tokens,
                bytes
        );
    }


    // ====================================================
    // READ LINE
    // ====================================================

    static String readLine(
            InputStream input
    ) throws IOException {

        return readLineCounting(input);
    }


    static String readLineCounting(
            InputStream input
    ) throws IOException {

        StringBuilder result =
                new StringBuilder();


        int previous = -1;


        while (true) {

            int current =
                    input.read();


            if (current == -1) {

                throw new EOFException(
                        "Connection closed"
                );
            }


            if (previous == '\r'
                    && current == '\n') {

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


    // ====================================================
    // READ EXACT NUMBER OF BYTES
    // ====================================================

    static byte[] readExactly(
            InputStream input,
            int length
    ) throws IOException {

        byte[] data =
                new byte[length];


        int total = 0;


        while (total < length) {

            int n =
                    input.read(
                            data,
                            total,
                            length - total
                    );


            if (n == -1) {

                throw new EOFException(
                        "Connection closed while reading"
                );
            }


            total += n;
        }


        return data;
    }


    // ====================================================
    // SEND
    // ====================================================

    static void send(
            OutputStream output,
            String command
    ) throws IOException {

        output.write(
                command.getBytes(
                        StandardCharsets.UTF_8
                )
        );

        output.flush();
    }


    // ====================================================
    // READ MASTER RESPONSE
    // ====================================================

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