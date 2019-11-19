package course.util.dataclasses;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import java.text.SimpleDateFormat;
import java.util.Locale;

/**
 * Class to Map the Twitter input into the TweetClass
 */
public class JsonNodeMapper {

    private transient ObjectMapper jsonParser;

    /**
     * Creates Class from Json
     *
     * @param json input json
     * @param tag needs Tag (Google, Facebook, ...)
     * @return TweetVlass
     * @throws Exception json reading
     */
    public TweetClass createNewClass(String json, String tag) throws Exception {
        if (jsonParser == null) {
            jsonParser = new ObjectMapper();
        }

        JsonNode jsonNode = jsonParser.readValue(json, JsonNode.class);
        TweetClass.RetweetClass retweetClass = null;
        TweetClass tweetClass = null;
        boolean isEnglish = getStringValue(jsonNode, "lang").equals("en");
        if (isEnglish) {

            long id = getLongValue(jsonNode, "id");
            String text = getStringValue(jsonNode, "text");
            String username = getStringValue(jsonNode.get("user"), "name");
            int followersCount = getIntValue(jsonNode.get("user"), "followers_count");
            int statusesCount = getIntValue(jsonNode.get("user"), "statuses_count");
            int favouritesCount = getIntValue(jsonNode.get("user"), "favourites_count");
            int retweetCount = getIntValue(jsonNode, "retweet_count");
            long createdAt = converTwitterTimestamp(getStringValue(jsonNode, "created_at"));
            String timestamp = converForKibana(getStringValue(jsonNode, "created_at"));

            if (jsonNode.has("retweeted_status")) {
                JsonNode retweetNode = jsonNode.get("retweeted_status");

                long idR = getLongValue(retweetNode, "id");
                String textR = getStringValue(retweetNode, "text");
                String usernameR = getStringValue(retweetNode.get("user"), "name");
                int followersCountR = getIntValue(retweetNode.get("user"), "followers_count");
                int favouritesCountR = getIntValue(retweetNode.get("user"), "favourites_count");
                int statusesCountR = getIntValue(retweetNode.get("user"), "statuses_count");
                int retweetCountR = getIntValue(retweetNode, "retweet_count");
                long createdAtR = converTwitterTimestamp(getStringValue(retweetNode, "created_at"));
                retweetClass = new TweetClass.RetweetClass(idR, textR, usernameR, followersCountR, favouritesCountR, statusesCountR, retweetCountR, createdAtR);
            }else {
                retweetClass = new TweetClass.RetweetClass();
            }
            tweetClass = new TweetClass(tag, id, text, username, followersCount, statusesCount, favouritesCount, retweetCount, createdAt, isEnglish, timestamp, retweetClass);

        }

        return tweetClass;
    }

    private String getStringValue(JsonNode jsonNode, String name){
        if (jsonNode.has(name)) {
            return jsonNode.get(name).asText();
        }else {
            return "";
        }
    }

    private int getIntValue(JsonNode jsonNode, String name){
        if (jsonNode.has(name)) {
            return jsonNode.get(name).asInt();
        }else {
            return 0;
        }
    }

    private long getLongValue(JsonNode jsonNode, String name){
        if (jsonNode.has(name)) {
            return jsonNode.get(name).asLong();
        }else {
            return 0L;
        }
    }

    private static String converForKibana(String date) throws Exception{
        final String tw = "EEE MMM dd HH:mm:ss ZZZZZ yyyy";
        SimpleDateFormat sf = new SimpleDateFormat(tw, Locale.ENGLISH);
        sf.setLenient(true);
        SimpleDateFormat format = new SimpleDateFormat("MMMM dd YYYY, HH:mm:ss.SSS");
        return format.format(sf.parse(date).getTime());
    }

    private static long converTwitterTimestamp(String date) throws Exception{
        final String tw = "EEE MMM dd HH:mm:ss ZZZZZ yyyy";
        SimpleDateFormat sf = new SimpleDateFormat(tw, Locale.ENGLISH);
        sf.setLenient(true);

        return sf.parse(date).getTime();
    }
}
