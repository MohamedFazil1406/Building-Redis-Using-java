import java.util.*;

public class CommandHandler {

    private final Map<String, String> data = new HashMap<>();
    private final Map<String, Long> expiry = new HashMap<>();
    private final Map<String, List<String>> lists = new HashMap<>();
    private final Object listLock = new Object();
    private final Map<String, List<StreamEntry>> streams = new HashMap<>();
    private long lastStreamTime = 0;
    private long lastStreamSequence = 0;

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

    public String handle(String[] tokens) {

        String command = tokens[0].toUpperCase();

        return switch (command) {

            case "PING" -> "+PONG\r\n";

            case "ECHO" ->
                    "$" + tokens[1].length() + "\r\n"
                            + tokens[1] + "\r\n";

            case "SET" -> {
                String key = tokens[1];
                String value = tokens[2];

                data.put(key, value);

                if (tokens.length >= 5) {

                    String option = tokens[3].toUpperCase();
                    long time = Long.parseLong(tokens[4]);

                    if (option.equals("EX")) {
                        expiry.put(key, System.currentTimeMillis() + time * 1000);
                    }
                    else if (option.equals("PX")) {
                        expiry.put(key, System.currentTimeMillis() + time);
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

                // Generate ID when "*"
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
                    String field = tokens[i];
                    String value = tokens[i + 1];

                    fields.put(field, value);
                }

                StreamEntry entry = new StreamEntry(id, fields);

                streams
                        .computeIfAbsent(key, k -> new ArrayList<>())
                        .add(entry);

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

            default -> "-ERR unknown command\r\n";
        };
    }
}