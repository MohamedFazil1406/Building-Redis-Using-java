import java.util.HashMap;
import java.util.Map;

public class CommandHandler {

    private final Map<String, String> data = new HashMap<>();

    public String handle(String[] tokens) {

        String command = tokens[0].toUpperCase();

        return switch (command) {

            case "PING" -> "+PONG\r\n";

            case "ECHO" ->
                    "$" + tokens[1].length() + "\r\n"
                            + tokens[1] + "\r\n";

            case "SET" -> {
                data.put(tokens[1], tokens[2]);
                yield "+OK\r\n";
            }

            case "GET" -> {
                String value = data.get(tokens[1]);

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