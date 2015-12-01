REGISTER file:/home/hadoop/lib/pig/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;
SET default_parallel 10;

-- Load the users, networks and tweets dataset
users= LOAD 's3://mrprojectsarika/samplenew.txt' using PigStorage(',') AS
(userId,userName,friendCount:int,followerCount:int,statusCount:int,favoriteCount:int,accountAge,city,state,longitude,latitude);
networks= LOAD 's3://mrprojectsarika/network.txt' using PigStorage(',') AS (followerId, friendId);
tweets= LOAD 's3://mrprojectsarika/tweets.txt' using PigStorage(',') AS (tuserId, status,originalText, copyText,link,tweetId,time,retweetCount:int,favorite,mentionedUserId,hashTags);

-- project the users dataset to set the longitude value to positive
projectedUsers= FOREACH users GENERATE userId,userName,friendCount,followerCount,statusCount,favoriteCount,state,-longitude AS timeZone;

-- Join the projected users dataset with Networks dataset and the resulting table with the tweets dataset
joinUsersAndNetworks= JOIN projectedUsers BY (userId), networks BY (followerId);
joinFollowersAndTweets= JOIN joinUsersAndNetworks BY (userId), tweets by (tuserId);

-- filter the joinFollowersAndTweets according to the longitude values
filterJoinTableCentral= FILTER joinFollowersAndTweets BY $7>75 AND $7< 105;
filterJoinTableEast= FILTER joinFollowersAndTweets BY $7<75;
filterJoinTableMountain= FILTER joinFollowersAndTweets BY $7>105 AND $7<120;
filterJoinTablePacific= FILTER joinFollowersAndTweets BY $7>120;

-- Project the each joined table to generate only the columns required to generate the character
projectedJoinCentral= FOREACH filterJoinTableCentral GENERATE $2 AS friendCount,$3 AS followerCount,$4 AS statusCount,$5 AS favoriteCount,$6 AS state,$17 AS retweetCount;
projectedJoinEast= FOREACH filterJoinTableEast GENERATE $2 AS friendCount,$3 AS followerCount,$4 AS statusCount,$5 AS favoriteCount,$6 AS state,$17 AS retweetCount;
projectedJoinMountain= FOREACH filterJoinTableMountain GENERATE $2 AS friendCount,$3 AS followerCount,$4 AS statusCount,$5 AS favoriteCount,$6 AS state,$17 AS retweetCount;
projectedJoinPacific= FOREACH filterJoinTablePacific GENERATE $2 AS friendCount,$3 AS followerCount,$4 AS statusCount,$5 AS favoriteCount,$6 AS state,$17 AS retweetCount;

-- group the projected table on the state
groupCentral= GROUP projectedJoinCentral BY state;
groupEast= GROUP projectedJoinEast BY state;
groupMountain= GROUP projectedJoinMountain BY state;
groupPacific= GROUP projectedJoinPacific BY state;

-- perform aggregation function on columns to determine the character of each state in each time zone
characterCentral= FOREACH groupCentral GENERATE group, MAX(projectedJoinCentral.retweetCount) AS maxRetweetCount,AVG(projectedJoinCentral.friendCount),AVG(projectedJoinCentral.followerCount),AVG(projectedJoinCentral.favoriteCount), AVG(projectedJoinCentral.statusCount);
characterEast= FOREACH groupEast GENERATE group, MAX(projectedJoinEast.retweetCount) AS maxRetweetCount,AVG(projectedJoinEast.friendCount),AVG(projectedJoinEast.followerCount),AVG(projectedJoinEast.favoriteCount), AVG(projectedJoinEast.statusCount);
characterMountain= FOREACH groupMountain GENERATE group, MAX(projectedJoinMountain.retweetCount) AS maxRetweetCount,AVG(projectedJoinMountain.friendCount),AVG(projectedJoinMountain.followerCount),AVG(projectedJoinMountain.favoriteCount), AVG(projectedJoinMountain.statusCount);
characterPacific= FOREACH groupPacific GENERATE group, MAX(projectedJoinPacific.retweetCount) AS maxRetweetCount,AVG(projectedJoinPacific.friendCount),AVG(projectedJoinPacific.followerCount),AVG(projectedJoinPacific.favoriteCount), AVG(projectedJoinPacific.statusCount);

-- Store the results in separate output folders 
STORE characterCentral INTO 's3://mrprojectsarika/outputJFP/central' USING PigStorage(',');
STORE characterEast INTO 's3://mrprojectsarika/outputJFP/east' USING PigStorage(',');
STORE characterMountain INTO 's3://mrprojectsarika/outputJFP/mountain' USING PigStorage(',');
STORE characterPacific INTO 's3://mrprojectsarika/outputJFP/pacific' USING PigStorage(',');


