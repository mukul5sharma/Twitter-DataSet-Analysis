REGISTER file:/home/hadoop/lib/pig/piggybank.jar
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;
SET default_parallel 10;

-- Load the users, networks and tweets dataset
users= LOAD 's3://mrprojectsarika/samplenew.txt' using PigStorage(',') AS
(userId,userName,friendCount:int,followerCount:int,statusCount:int,favoriteCount:int,accountAge,city,state,longitude,latitude);
networks= LOAD 's3://mrprojectsarika/network.txt' using PigStorage(',') AS (followerId, friendId);
tweets= LOAD 's3://mrprojectsarika/tweets.txt' using PigStorage(',') AS (tuserId, status,originalText, copyText,link,tweetId,time,retweetCount:int,favorite,mentionedUserId,hashTags);

-- project the users dataset to set the longitude value to positive
projectUser= FOREACH users GENERATE userId,userName,friendCount,followerCount,statusCount,favoriteCount,state,-longitude AS timezone;

-- filter the users dataset according to the longitude values
filterUsersCentral= FILTER projectUser BY timezone >=75 AND $7< 105;
filterUsersEast= FILTER projectUser BY timezone <75;
filterUsersMountain= FILTER projectUser BY timezone >=105 AND $7< 120;
filterUsersPacific= FILTER projectUser BY timezone >=120;

-- Join the each filtered users dataset with Networks dataset and the resulting table with the tweets dataset
joinUsersCentralAndNetworks= JOIN filterUsersCentral BY (userId), networks BY (followerId);
joinFollowersAndTweetsCentral= JOIN joinUsersCentralAndNetworks BY (userId), tweets by (tuserId);

joinUsersEastAndNetworks= JOIN filterUsersEast BY (userId), networks BY (followerId);
joinFollowersAndTweetsEast= JOIN joinUsersEastAndNetworks BY (userId), tweets by (tuserId);

joinUsersMountainAndNetworks= JOIN filterUsersMountain BY (userId), networks BY (followerId);
joinFollowersAndTweetsMountain= JOIN joinUsersMountainAndNetworks BY (userId), tweets by (tuserId);

joinUsersPacificAndNetworks= JOIN filterUsersPacific BY (userId), networks BY (followerId);
joinFollowersAndTweetsPacific= JOIN joinUsersPacificAndNetworks BY (userId), tweets by (tuserId);

-- Project the joined table to generate only the columns required to generate the character
projectCentral= FOREACH joinFollowersAndTweetsCentral GENERATE $2 AS friendCount,$3 AS followerCount,$4 AS statusCount,$5 AS favoriteCount,$6 AS state,$17 AS retweetCount;
projectEast= FOREACH joinFollowersAndTweetsEast GENERATE $2 AS friendCount,$3 AS followerCount,$4 AS statusCount,$5 AS favoriteCount,$6 AS state,$17 AS retweetCount;
projectMountain= FOREACH joinFollowersAndTweetsMountain GENERATE $2 AS friendCount,$3 AS followerCount,$4 AS statusCount,$5 AS favoriteCount,$6 AS state,$17 AS retweetCount;
projectPacific= FOREACH joinFollowersAndTweetsPacific GENERATE $2 AS friendCount,$3 AS followerCount,$4 AS statusCount,$5 AS favoriteCount,$6 AS state,$17 AS retweetCount;

-- group the projected table on the state
groupCentral= GROUP projectCentral BY state;
groupEast= GROUP projectEast BY state;
groupMountain= GROUP projectMountain BY state;
groupPacific= GROUP projectPacific BY state;

-- perform aggregation function on columns to determine the character of each state in each time zone
characterCentral= FOREACH groupCentral GENERATE group, MAX(projectCentral.retweetCount), AVG(projectCentral.friendCount),
AVG(projectCentral.followerCount),AVG(projectCentral.favoriteCount), AVG(projectCentral.statusCount);

characterEast= FOREACH groupEast GENERATE group, MAX(projectEast.retweetCount), AVG(projectEast.friendCount),
AVG(projectEast.followerCount),AVG(projectEast.favoriteCount), AVG(projectEast.statusCount);

characterMountain= FOREACH groupMountain GENERATE group, MAX(projectMountain.retweetCount), AVG(projectMountain.friendCount),
AVG(projectMountain.followerCount),AVG(projectMountain.favoriteCount), AVG(projectMountain.statusCount);

characterPacific= FOREACH groupPacific GENERATE group, MAX(projectPacific.retweetCount), AVG(projectPacific.friendCount),
AVG(projectPacific.followerCount),AVG(projectPacific.favoriteCount), AVG(projectPacific.statusCount);

-- Store the results in separate output folders 
STORE characterCentral INTO 's3://mrprojectsarika/outputFJP/central' USING PigStorage(',');
STORE characterEast INTO 's3://mrprojectsarika/outputFJP/east' USING PigStorage(',');
STORE characterMountain INTO 's3://mrprojectsarika/outputFJP/mountain' USING PigStorage(',');
STORE characterPacific INTO 's3://mrprojectsarika/outputFJP/pacific' USING PigStorage(',');

