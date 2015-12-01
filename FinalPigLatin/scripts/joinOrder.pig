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

-- Join the Networks dataset with the projected users dataset and the resulting table with the tweets dataset
joinUsersAndNetworks= JOIN  networks BY (followerId),projectedUsers BY (userId);
joinFollowersAndTweets= JOIN tweets by (tuserId),joinUsersAndNetworks BY (userId);

-- Project the joined table to generate only the columns required to generate the character
projectedJoin= FOREACH joinFollowersAndTweets GENERATE joinUsersAndNetworks::projectedUsers::userId AS userId, joinUsersAndNetworks::projectedUsers::friendCount AS friendCount, joinUsersAndNetworks::projectedUsers::followerCount AS followerCount, joinUsersAndNetworks::projectedUsers::statusCount AS statusCount, joinUsersAndNetworks::projectedUsers::favoriteCount AS favoriteCount, joinUsersAndNetworks::projectedUsers::state AS state,joinUsersAndNetworks::projectedUsers::timeZone AS timeZone, tweets::retweetCount AS retweetCount;

-- Split the joined table into time zones. I have used SPLIT command instead of the FILTER command
SPLIT projectedJoin INTO splitCentral IF (timeZone>75 AND timeZone< 105), splitEast IF (timeZone<75), splitMountain IF (timeZone>105 AND timeZone< 120), splitPacific IF (timeZone>120);

-- group the filtered table on the state
groupCentral= GROUP splitCentral BY (state);
groupEast= GROUP splitEast BY (state);
groupMountain= GROUP splitMountain BY (state);
groupPacific= GROUP splitPacific BY (state);

-- perform aggregation function on columns to determine the character of each state in each time zone
characterCentral= FOREACH groupCentral GENERATE group, MAX(splitCentral.retweetCount),AVG(splitCentral.friendCount),AVG(splitCentral.followerCount),AVG(splitCentral.favoriteCount), AVG(splitCentral.statusCount);
characterEast= FOREACH groupEast GENERATE group, MAX(splitEast.retweetCount),AVG(splitEast.friendCount),AVG(splitEast.followerCount),AVG(splitEast.favoriteCount), AVG(splitEast.statusCount);
characterMountain= FOREACH groupMountain GENERATE group, MAX(splitMountain.retweetCount),AVG(splitMountain.friendCount),AVG(splitMountain.followerCount),AVG(splitMountain.favoriteCount), AVG(splitMountain.statusCount);
characterPacific= FOREACH groupPacific GENERATE group, MAX(splitPacific.retweetCount),AVG(splitPacific.friendCount),AVG(splitPacific.followerCount),AVG(splitPacific.favoriteCount), AVG(splitPacific.statusCount);

-- Store the results in separate output folders 
STORE characterCentral INTO 's3://mrprojectsarika/outputOrder/central' USING PigStorage(',');
STORE characterEast INTO 's3://mrprojectsarika/outputOrder/east' USING PigStorage(',');
STORE characterMountain INTO 's3://mrprojectsarika/outputOrder/mountain' USING PigStorage(',');
STORE characterPacific INTO 's3://mrprojectsarika/outputOrder/pacific' USING PigStorage(',');

