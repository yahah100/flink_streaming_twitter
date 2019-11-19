package course.util.dataclasses;

/**
 * DataClass for one Tweet
 */
public class TweetClass {


    public String tag;
    public long id;
    public String text;
    public String username;
    public int followersCount;
    public int statusesCount;
    public int favouritesCount;
    public int retweetCount;
    public long createdAt;
    public final boolean isEnglish;
    public String timestamp;
    public final RetweetClass retweet;


    public TweetClass(String tag, long id, String text, String username, int followersCount, int statusesCount, int favouritesCount, int retweetCount, long createdAt, boolean isEnglish, String timestamp, RetweetClass retweet) {
        this.tag = tag;
        this.id = id;
        this.text = text;
        this.username = username;
        this.followersCount = followersCount;
        this.statusesCount = statusesCount;
        this.favouritesCount = favouritesCount;
        this.retweetCount = retweetCount;
        this.createdAt = createdAt;
        this.isEnglish = isEnglish;
        this.timestamp = timestamp;
        this.retweet = retweet;
    }

    public TweetClass() {
        this.tag = "";
        this.id = 0L;
        this.text = "";
        this.username = "";
        this.followersCount = 0;
        this.statusesCount = 0;
        this.favouritesCount = 0;
        this.retweetCount = 0;
        this.createdAt = 0L;
        this.isEnglish = false;
        this.timestamp = "";
        this.retweet = new RetweetClass();
    }

    public static class RetweetClass {
        public long id;
        public String text;
        public String username;
        public int followersCount;
        public int statusesCount;
        public int favouritesCount;
        public int retweetCount;
        public long createdAt;

        public RetweetClass(long id, String text, String username, int followersCount, int statusesCount, int favouritesCount, int retweetCount, long createdAt) {
            this.id = id;
            this.text = text;
            this.username = username;
            this.followersCount = followersCount;
            this.statusesCount = statusesCount;
            this.favouritesCount = favouritesCount;
            this.retweetCount = retweetCount;
            this.createdAt = createdAt;
        }

        public RetweetClass() {
            this.id = 0L;
            this.text = "";
            this.username = "";
            this.followersCount = 0;
            this.statusesCount = 0;
            this.favouritesCount = 0;
            this.retweetCount = 0;
            this.createdAt = 0L;
        }
    }

}
