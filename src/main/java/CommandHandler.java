import java.util.*;

public class CommandHandler {

    private final Map<String, String> data = new HashMap<>();
    private final Map<String, Long> expiry = new HashMap<>();
    private final Map<String, List<String>> lists = new HashMap<>();
    private final Object listLock = new Object();
    private final Map<String, List<StreamEntry>> streams = new HashMap<>();
    private final Object streamLock = new Object();
    private long lastStreamTime = 0;
    private long lastStreamSequence = 0;
    private boolean inTransaction = false;
    private boolean transactionAborted = false;
    private final List<String[]> transactionQueue = new ArrayList<>();
    private final Set<String> watchedKeys = new HashSet<>();
    private final boolean isReplica;
    private final String replicationId;
    private final long replicationOffset;

    public CommandHandler(boolean isReplica, String replicationId,
                          long replicationOffset) {

        this.isReplica = isReplica;
        this.replicationId = replicationId;
        this.replicationOffset = replicationOffset;
    }

    private int compareStreamIds(String a, String b) {

        String[] first = a.split("-");
        String[] second = b.split("-");

        long timeA = Long.parseLong(first[0]);
        long sequenceA = Long.parseLong(first[1]);

        long timeB = Long.parseLong(second[0]);
        long sequenceB = Long.parseLong(second[1]);

        if (timeA != timeB) {
            return Long.compare(timeA, timeB);
        }

        return Long.compare(sequenceA, sequenceB);
    }

    private String buildXReadResponse(
            String[] keys,
            String[] startIds) {

        StringBuilder response = new StringBuilder();

        int streamCount = 0;

        for (int i = 0; i < keys.length; i++) {

            String key = keys[i];
            String startId = startIds[i];

            List<StreamEntry> entries = streams.get(key);

            if (entries == null) {
                continue;
            }

            List<StreamEntry> result = new ArrayList<>();

            for (StreamEntry entry : entries) {

                if (compareStreamIds(
                        entry.getId(),
                        startId) > 0) {

                    result.add(entry);
                }
            }

            if (result.isEmpty()) {
                continue;
            }

            streamCount++;

            // [stream name, entries]
            response.append("*2\r\n");

            // Stream name
            response.append("$")
                    .append(key.length())
                    .append("\r\n")
                    .append(key)
                    .append("\r\n");

            // Entries array
            response.append("*")
                    .append(result.size())
                    .append("\r\n");

            for (StreamEntry entry : result) {

                // [id, fields]
                response.append("*2\r\n");

                String id = entry.getId();

                // ID
                response.append("$")
                        .append(id.length())
                        .append("\r\n")
                        .append(id)
                        .append("\r\n");

                Map<String, String> fields =
                        entry.getFields();

                // Field/value array
                response.append("*")
                        .append(fields.size() * 2)
                        .append("\r\n");

                for (Map.Entry<String, String> field :
                        fields.entrySet()) {

                    String fieldName = field.getKey();
                    String value = field.getValue();

                    // Field
                    response.append("$")
                            .append(fieldName.length())
                            .append("\r\n")
                            .append(fieldName)
                            .append("\r\n");

                    // Value
                    response.append("$")
                            .append(value.length())
                            .append("\r\n")
                            .append(value)
                            .append("\r\n");
                }
            }
        }

        if (streamCount == 0) {
            return null;
        }

        return "*" + streamCount + "\r\n"
                + response;
    }

    public String handle(String[] tokens) {

        String command = tokens[0].toUpperCase();

        if (inTransaction
                && !command.equals("EXEC")
                && !command.equals("DISCARD")) {

            transactionQueue.add(tokens);

            return "+QUEUED\r\n";
        }

        return switch (command) {

            case "PING" -> "+PONG\r\n";

            case "ECHO" ->
                    "$" + tokens[1].length() + "\r\n"
                            + tokens[1] + "\r\n";

            case "SET" -> {

                String key = tokens[1];
                String value = tokens[2];

                data.put(key, value);

                if (watchedKeys.contains(key) && !inTransaction) {
                    transactionAborted = true;
                }

                if (tokens.length >= 5) {

                    String option = tokens[3].toUpperCase();
                    long time = Long.parseLong(tokens[4]);

                    if (option.equals("EX")) {
                        expiry.put(
                                key,
                                System.currentTimeMillis() + time * 1000
                        );
                    }
                    else if (option.equals("PX")) {
                        expiry.put(
                                key,
                                System.currentTimeMillis() + time
                        );
                    }
                }

                yield "+OK\r\n";
            }

            case "GET" -> {

                String key = tokens[1];

                if (expiry.containsKey(key)) {

                    long expirationTime = expiry.get(key);

                    if (System.currentTimeMillis() >= expirationTime) {

                        data.remove(key);
                        expiry.remove(key);

                        yield "$-1\r\n";
                    }
                }

                String value = data.get(key);

                if (value == null) {
                    yield "$-1\r\n";
                }

                yield "$" + value.length() + "\r\n"
                        + value + "\r\n";
            }

            case "RPUSH" -> {
                String key = tokens[1];

                synchronized (listLock) {

                    List<String> list =
                            lists.computeIfAbsent(key, k -> new ArrayList<>());

                    for (int i = 2; i < tokens.length; i++) {
                        list.add(tokens[i]);
                    }

                    listLock.notifyAll();

                    yield ":" + list.size() + "\r\n";
                }
            }

            case "LRANGE" -> {
                String key = tokens[1];

                int start = Integer.parseInt(tokens[2]);
                int stop = Integer.parseInt(tokens[3]);

                List<String> list = lists.get(key);

                if (list == null) {
                    yield "*0\r\n";
                }

                // Handle negative indexes
                if (start < 0) {
                    start = list.size() + start;
                }

                if (stop < 0) {
                    stop = list.size() + stop;
                }

                // Keep indexes within bounds
                start = Math.max(start, 0);
                stop = Math.min(stop, list.size() - 1);

                if (start > stop || start >= list.size()) {
                    yield "*0\r\n";
                }

                StringBuilder response = new StringBuilder();

                int count = stop - start + 1;

                response.append("*").append(count).append("\r\n");

                for (int i = start; i <= stop; i++) {
                    String value = list.get(i);

                    response.append("$")
                            .append(value.length())
                            .append("\r\n")
                            .append(value)
                            .append("\r\n");
                }

                yield response.toString();
            }

            case "LPUSH" -> {
                String key = tokens[1];

                List<String> list =
                        lists.computeIfAbsent(key, k -> new ArrayList<>());

                for (int i = 2; i < tokens.length; i++) {
                    list.add(0, tokens[i]);
                }

                yield ":" + list.size() + "\r\n";
            }

            case "LLEN" -> {
                String key = tokens[1];

                List<String> list = lists.get(key);

                if (list == null) {
                    yield ":0\r\n";
                }

                yield ":" + list.size() + "\r\n";
            }

            case "LPOP" -> {
                String key = tokens[1];

                List<String> list = lists.get(key);

                if (list == null || list.isEmpty()) {
                    yield "$-1\r\n";
                }

                String value = list.remove(0);

                yield "$" + value.length() + "\r\n"
                        + value + "\r\n";
            }

            case "RPOP" -> {
                String key = tokens[1];

                List<String> list = lists.get(key);

                if (list == null || list.isEmpty()) {
                    yield "$-1\r\n";
                }

                String value = list.remove(list.size() - 1);

                yield "$" + value.length() + "\r\n"
                        + value + "\r\n";
            }

            case "BLPOP" -> {
                String key = tokens[1];
                long timeout = Long.parseLong(tokens[2]);

                synchronized (listLock) {

                    List<String> list =
                            lists.computeIfAbsent(key, k -> new ArrayList<>());

                    long timeoutMillis = timeout * 1000;
                    long startTime = System.currentTimeMillis();

                    while (list.isEmpty()) {

                        long elapsed =
                                System.currentTimeMillis() - startTime;

                        long remaining =
                                timeoutMillis - elapsed;

                        if (remaining <= 0) {
                            yield "*-1\r\n";
                        }

                        try {
                            listLock.wait(remaining);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            yield "*-1\r\n";
                        }
                    }

                    String value = list.remove(0);

                    yield "*2\r\n"
                            + "$" + key.length() + "\r\n"
                            + key + "\r\n"
                            + "$" + value.length() + "\r\n"
                            + value + "\r\n";
                }
            }


            case "TYPE" -> {
                String key = tokens[1];

                if (data.containsKey(key)) {
                    yield "+string\r\n";
                }

                if (lists.containsKey(key)) {
                    yield "+list\r\n";
                }

                yield "+none\r\n";
            }
            case "XADD" -> {
                String key = tokens[1];
                String id = tokens[2];

                if (id.equals("*")) {
                    long currentTime = System.currentTimeMillis();

                    if (currentTime == lastStreamTime) {
                        lastStreamSequence++;
                    } else {
                        lastStreamTime = currentTime;
                        lastStreamSequence = 0;
                    }

                    id = lastStreamTime + "-" + lastStreamSequence;
                }

                Map<String, String> fields = new LinkedHashMap<>();

                for (int i = 3; i < tokens.length; i += 2) {
                    fields.put(tokens[i], tokens[i + 1]);
                }

                StreamEntry entry = new StreamEntry(id, fields);

                synchronized (streamLock) {
                    streams
                            .computeIfAbsent(key, k -> new ArrayList<>())
                            .add(entry);

                    // Wake up blocked XREAD clients
                    streamLock.notifyAll();
                }

                yield "$" + id.length() + "\r\n"
                        + id + "\r\n";
            }

            case "XRANGE" -> {
                String key = tokens[1];
                String start = tokens[2];
                String end = tokens[3];

                List<StreamEntry> entries = streams.get(key);

                if (entries == null) {
                    yield "*0\r\n";
                }

                List<StreamEntry> result = new ArrayList<>();

                for (StreamEntry entry : entries) {

                    String id = entry.getId();

                    if (compareStreamIds(id, start) >= 0 &&
                            compareStreamIds(id, end) <= 0) {

                        result.add(entry);
                    }
                }

                StringBuilder response = new StringBuilder();

                response.append("*")
                        .append(result.size())
                        .append("\r\n");

                for (StreamEntry entry : result) {

                    response.append("*2\r\n");

                    String id = entry.getId();

                    response.append("$")
                            .append(id.length())
                            .append("\r\n")
                            .append(id)
                            .append("\r\n");

                    Map<String, String> fields = entry.getFields();

                    response.append("*")
                            .append(fields.size() * 2)
                            .append("\r\n");

                    for (Map.Entry<String, String> field : fields.entrySet()) {

                        response.append("$")
                                .append(field.getKey().length())
                                .append("\r\n")
                                .append(field.getKey())
                                .append("\r\n");

                        response.append("$")
                                .append(field.getValue().length())
                                .append("\r\n")
                                .append(field.getValue())
                                .append("\r\n");
                    }
                }

                yield response.toString();
            }

            case "XREAD" -> {

                boolean blocking = false;
                long blockTime = 0;

                int streamsIndex = -1;

                // Find BLOCK and STREAMS
                for (int i = 1; i < tokens.length; i++) {

                    if (tokens[i].equalsIgnoreCase("BLOCK")) {
                        blocking = true;
                        blockTime = Long.parseLong(tokens[i + 1]);
                    }

                    if (tokens[i].equalsIgnoreCase("STREAMS")) {
                        streamsIndex = i;
                        break;
                    }
                }

                if (streamsIndex == -1) {
                    yield "-ERR syntax error\r\n";
                }

                int numberOfStreams =
                        (tokens.length - streamsIndex - 1) / 2;

                String[] keys = new String[numberOfStreams];
                String[] startIds = new String[numberOfStreams];

                // Read stream names
                for (int i = 0; i < numberOfStreams; i++) {
                    keys[i] = tokens[streamsIndex + 1 + i];
                }

                // Read IDs
                for (int i = 0; i < numberOfStreams; i++) {
                    startIds[i] =
                            tokens[streamsIndex + 1 + numberOfStreams + i];
                }

                long startTime = System.currentTimeMillis();

                synchronized (streamLock) {

                    while (true) {

                        String response =
                                buildXReadResponse(keys, startIds);

                        // We found new entries
                        if (response != null) {
                            yield response;
                        }

                        // Normal XREAD -> don't wait
                        if (!blocking) {
                            yield "*0\r\n";
                        }

                        // BLOCK 0 -> wait forever
                        if (blockTime == 0) {

                            try {
                                streamLock.wait();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                yield "*0\r\n";
                            }

                        } else {

                            long elapsed =
                                    System.currentTimeMillis() - startTime;

                            long remaining =
                                    blockTime - elapsed;

                            // Timeout
                            if (remaining <= 0) {
                                yield "*-1\r\n";
                            }

                            try {
                                streamLock.wait(remaining);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                                yield "*0\r\n";
                            }
                        }
                    }
                }
            }

            case "INCR" -> {
                String key = tokens[1];

                String value = data.getOrDefault(key, "0");

                try {
                    long number = Long.parseLong(value);
                    number++;

                    data.put(key, String.valueOf(number));

                    yield ":" + number + "\r\n";

                } catch (NumberFormatException e) {
                    yield "-ERR value is not an integer or out of range\r\n";
                }
            }
            case "MULTI" -> {

                inTransaction = true;
                transactionAborted = false;

                yield "+OK\r\n";
            }
            case "EXEC" -> {

                if (!inTransaction) {
                    yield "-ERR EXEC without MULTI\r\n";
                }

                // A watched key was modified
                if (transactionAborted) {

                    inTransaction = false;
                    transactionAborted = false;

                    transactionQueue.clear();
                    watchedKeys.clear();

                    yield "*-1\r\n";
                }

                StringBuilder response = new StringBuilder();

                response.append("*")
                        .append(transactionQueue.size())
                        .append("\r\n");

                inTransaction = false;

                for (String[] queuedCommand : transactionQueue) {
                    response.append(handle(queuedCommand));
                }

                transactionQueue.clear();
                watchedKeys.clear();

                yield response.toString();
            }
            case "DISCARD" -> {

                if (!inTransaction) {
                    yield "-ERR DISCARD without MULTI\r\n";
                }

                inTransaction = false;
                transactionAborted = false;

                transactionQueue.clear();
                watchedKeys.clear();

                yield "+OK\r\n";
            }

            case "WATCH" -> {
                for (int i = 1; i < tokens.length; i++) {
                    watchedKeys.add(tokens[i]);
                }

                yield "+OK\r\n";
            }
            case "UNWATCH" -> {

                watchedKeys.clear();
                transactionAborted = false;

                yield "+OK\r\n";
            }

            case "INFO" -> {

                if (tokens.length > 1 &&
                        tokens[1].equalsIgnoreCase("replication")) {

                    String response =
                            "# Replication\r\n" +
                                    (isReplica
                                            ? "role:slave\r\n"
                                            : "role:master\r\n") +
                                    "master_replid:" + replicationId + "\r\n" +
                                    "master_repl_offset:" + replicationOffset + "\r\n";

                    yield "$" + response.length() +
                            "\r\n" +
                            response +
                            "\r\n";
                }

                yield "";
            }
            case "REPLCONF" -> {

                if (tokens.length >= 2 &&
                        tokens[1].equalsIgnoreCase("GETACK")) {

                    yield "*3\r\n" +
                            "$8\r\nREPLCONF\r\n" +
                            "$3\r\nACK\r\n" +
                            "$1\r\n0\r\n";
                }

                yield "+OK\r\n";
            }
            case "PSYNC" -> {
                yield "+FULLRESYNC " + replicationId + " " + replicationOffset + "\r\n";
            }

            default -> "-ERR unknown command\r\n";
        };
    }
}