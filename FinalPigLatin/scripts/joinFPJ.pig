REGISTER file:/home/hadoop/lib/pig/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;
SET default_parallel 10;

--Load the UserProfile dataset
users= LOAD 's3://mrprojectsarika/samplenew.txt' using PigStorage(',') AS
(userId,userName,friendCount:int,followerCount:int,statusCount:int,favoriteCount:int,accountAge,city,state,longitude,latitude);

-- Project Users table so that we can get a positive longitude value 
projectUser= FOREACH users GENERATE userId,userName,friendCount,followerCount,statusCount,favoriteCount,state,-longitude AS timezone;

--Load the Networks dataset
networks= LOAD 's3://mrprojectsarika/network.txt' using PigStorage(',') AS (followerId, friendId);

--Load the Tweets dataset
tweets= LOAD 's3://mrprojectsarika/tweets.txt' using PigStorage(',') AS (tuserId, status,originalText, copyText,link,tweetId,time,retweetCount:int,favorite,mentionedUserId,hashTags);

-- Filter the User table based on longitude into different time zones
filterUsersCentral= FILTER projectUser BY timezone >=75 AND $7< 105;
filterUsersEast= FILTER projectUser BY timezone <75;
filterUsersMountain= FILTER projectUser BY timezone >=105 AND $7< 120;
filterUsersPacific= FILTER projectUser BY timezone >=120;

-- Project the filtered UserProfile, networks and tweets data set
projectedUsersCentral= FOREACH filterUsersCentral GENERATE userId,userName,friendCount,followerCount,statusCount,favoriteCount,state,timezone;
projectedUsersEast= FOREACH filterUsersEast GENERATE userId,userName,friendCount,followerCount,statusCount,favoriteCount,state,timezone;
projectedUsersMountain= FOREACH filterUsersMountain GENERATE userId,userName,friendCount,followerCount,statusCount,favoriteCount,state,timezone;
projectedUsersPacific= FOREACH filterUsersPacific GENERATE userId,userName,friendCount,followerCount,statusCount,favoriteCount,state,timezone;

projectedNetworks= FOREACH networks GENERATE followerId;
projectedTweets= FOREACH tweets GENERATE tuserId,originalText,retweetCount,hashTags;

-- Join the projected user profile dataset with the Networks table on userId to get the records of only the followers
joinUsersCentralAndNetworks= JOIN projectedUsersCentral BY (userId), projectedNetworks BY (followerId);
-- Join the previously joined results with the projected tweets table to get tweet information of the followers
joinFollowersAndTweetsCentral= JOIN joinUsersCentralAndNetworks BY (userId), projectedTweets by (tuserId);

joinUsersEastAndNetworks= JOIN projectedUsersEast BY (userId), projectedNetworks BY (followerId);
joinFollowersAndTweetsEast= JOIN joinUsersEastAndNetworks BY (userId), projectedTweets by (tuserId);

joinUsersMountainAndNetworks= JOIN projectedUsersMountain BY (userId), projectedNetworks BY (followerId);
joinFollowersAndTweetsMountain= JOIN joinUsersMountainAndNetworks BY (userId), projectedTweets by (tuserId);

joinUsersPacificAndNetworks= JOIN projectedUsersPacific BY (userId), projectedNetworks BY (followerId);
joinFollowersAndTweetsPacific= JOIN joinUsersPacificAndNetworks BY (userId), projectedTweets by (tuserId);

-- Group each joined table by state for each time zone
groupDataCentral= GROUP joinFollowersAndTweetsCentral BY (state);
groupDataEast= GROUP joinFollowersAndTweetsEast BY (state);
groupDataMountain= GROUP joinFollowersAndTweetsMountain BY (state);
groupDataPacific= GROUP joinFollowersAndTweetsPacific BY (state);

-- Determine the character of the followers in each time zone by applying aggregate functions 
characterCentral= FOREACH groupDataCentral GENERATE group, MAX($1.$11) AS maximumRetweets,AVG($1.$2),AVG($1.$3), AVG($1.$4), AVG($1.$5);
characterEast= FOREACH groupDataEast GENERATE group, MAX($1.$11) AS maximumRetweets,AVG($1.$2),AVG($1.$3), AVG($1.$4), AVG($1.$5);
characterMountain= FOREACH groupDataMountain GENERATE group, MAX($1.$11) AS maximumRetweets,AVG($1.$2),AVG($1.$3), AVG($1.$4), AVG($1.$5);
characterPacific= FOREACH groupDataPacific GENERATE group, MAX($1.$11) AS maximumRetweets,AVG($1.$2),AVG($1.$3), AVG($1.$4), AVG($1.$5);

-- Store the results for each time zone
STORE characterCentral INTO 's3://mrprojectsarika/outputFPJ/central' USING PigStorage(',');
STORE characterEast INTO 's3://mrprojectsarika/outputFPJ/east' USING PigStorage(',');
STORE characterMountain INTO 's3://mrprojectsarika/outputFPJ/mountain' USING PigStorage(',');
STORE characterPacific INTO 's3://mrprojectsarika/outputFPJ/pacific' USING PigStorage(',');
