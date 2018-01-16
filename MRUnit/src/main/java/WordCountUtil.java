import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WordCountUtil {

    private static List<String> stopList = new ArrayList<String>(Arrays.asList("a","the","this","it","there","can","be"));

    public static List<String> getStopList(){
        return stopList;
    }
}
