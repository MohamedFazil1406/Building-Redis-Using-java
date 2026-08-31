import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommandHandler {

    private final Map<String, String> data = new HashMap<>();
    private final Map<String, Long> expiry = new HashMap<>();
    private final Map<String, List<String>> lists = new HashMap<>();

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

                List<String> list =
                        lists.computeIfAbsent(key, k -> new ArrayList<>());

                for (int i = 2; i < tokens.length; i++) {
                    list.add(tokens[i]);
                }

                yield ":" + list.size() + "\r\n";
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

            default -> "-ERR unknown command\r\n";
        };
    }
}