import java.util.HashMap;
import java.util.Map;

public class CommandHandler {

    private final Map<String, String> data = new HashMap<>();
    private final Map<String, Long> expiry = new HashMap<>();

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

            default -> "-ERR unknown command\r\n";
        };
    }
}