REGISTER file:/home/hadoop/lib/pig/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;
SET default_parallel 10;

-- Load the users, networks and tweets dataset
users= LOAD 's3://mrprojectsarika/samplenew.txt' using PigStorage(',') AS
(userId,userName,friendCount:int,followerCount:int,statusCount:int,favoriteCount:int,accountAge,city,state,longitude,latitude);
networks= LOAD 's3://mrprojectsarika/network.txt' using PigStorage(',') AS (followerId, friendId);
tweets= LOAD 's3://mrprojectsarika/tweets.txt' using PigStorage(',') AS (tuserId, status,originalText, copyText,link,tweetId,time,retweetCount:int,favorite,mentionedUserId,hashTags);

-- Project each dataset to generate only the required columns
projectedUsers= FOREACH users GENERATE userId,userName,friendCount,followerCount,statusCount,favoriteCount,state,-longitude AS timeZone;
projectedNetworks= FOREACH networks GENERATE followerId;
projectedTweets= FOREACH tweets GENERATE tuserId,originalText,retweetCount,hashTags;

-- Filter the projectedUsers table into time zones. 
filterProjectedUsersCentral= FILTER projectedUsers BY timeZone>75 AND timeZone< 105;
filterProjectedUsersEast= FILTER projectedUsers BY timeZone<75;
filterProjectedUsersMountain= FILTER projectedUsers BY timeZone>105 AND timeZone<120;
filterProjectedUsersPacific= FILTER projectedUsers BY timeZone>120;

-- Join each filtered users dataset with the Networks dataset and the resulting table with the tweets dataset
joinUsersCentralAndNetworks= JOIN filterProjectedUsersCentral BY (userId), projectedNetworks BY (followerId);
joinFollowersAndTweetsCentral= JOIN joinUsersCentralAndNetworks BY (userId), projectedTweets by (tuserId);

joinUsersEastAndNetworks= JOIN filterProjectedUsersEast BY (userId), projectedNetworks BY (followerId);
joinFollowersAndTweetsEast= JOIN joinUsersEastAndNetworks BY (userId), projectedTweets by (tuserId);

joinUsersMountainAndNetworks= JOIN filterProjectedUsersMountain BY (userId), projectedNetworks BY (followerId);
joinFollowersAndTweetsMountain= JOIN joinUsersMountainAndNetworks BY (userId), projectedTweets by (tuserId);


joinUsersPacificAndNetworks= JOIN filterProjectedUsersPacific BY (userId), projectedNetworks BY (followerId);
joinFollowersAndTweetsPacific= JOIN joinUsersPacificAndNetworks BY (userId), projectedTweets by (tuserId);

-- group the filtered table on the state
groupDataCentral= GROUP joinFollowersAndTweetsCentral BY (state);
groupDataEast= GROUP joinFollowersAndTweetsEast BY (state);
groupDataMountain= GROUP joinFollowersAndTweetsMountain BY (state);
groupDataPacific= GROUP joinFollowersAndTweetsPacific BY (state);

-- perform aggregation function on columns to determine the character of each state in each time zone
characterCentral= FOREACH groupDataCentral GENERATE group, MAX($1.$11) AS maximumRetweets,AVG($1.$2),AVG($1.$3), AVG($1.$4), AVG($1.$5);
characterEast= FOREACH groupDataEast GENERATE group, MAX($1.$11) AS maximumRetweets,AVG($1.$2),AVG($1.$3), AVG($1.$4), AVG($1.$5);
characterMountain= FOREACH groupDataMountain GENERATE group, MAX($1.$11) AS maximumRetweets,AVG($1.$2),AVG($1.$3), AVG($1.$4), AVG($1.$5);
characterPacific= FOREACH groupDataPacific GENERATE group, MAX($1.$11) AS maximumRetweets,AVG($1.$2),AVG($1.$3), AVG($1.$4), AVG($1.$5);

-- Store the results in separate output folders 
STORE characterCentral INTO 's3://mrprojectsarika/outputPFJ/central' USING PigStorage(',');
STORE characterEast INTO 's3://mrprojectsarika/outputPFJ/east' USING PigStorage(',');
STORE characterMountain INTO 's3://mrprojectsarika/outputPFJ/mountain' USING PigStorage(',');
STORE characterPacific INTO 's3://mrprojectsarika/outputPFJ/pacific' USING PigStorage(',');



