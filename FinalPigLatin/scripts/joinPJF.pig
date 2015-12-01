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

-- Join the projectedUsers table with projetedNetwroks and the resulting joinedTable with the projectedTweets table.
joinUsersAndNetworks= JOIN projectedUsers BY (userId), projectedNetworks BY (followerId);
joinFollowersAndTweets= JOIN joinUsersAndNetworks BY (userId), projectedTweets by (tuserId);

--Filtering on Time Zones Central, group by state and perform aggregation to determine the character
filterCentral= FILTER joinFollowersAndTweets BY $7>75 AND $7< 105;
groupDataCentral= GROUP filterCentral BY (state);
characterCentral= FOREACH groupDataCentral GENERATE group,MAX(filterCentral.retweetCount) AS maximumRetweets, AVG(filterCentral.friendCount),
AVG(filterCentral.followerCount),AVG(filterCentral.favoriteCount), AVG(filterCentral.statusCount);

--Filtering on Time Zones East, group by state and perform aggregation to determine the character
filterEast= FILTER joinFollowersAndTweets BY $7<75;
groupDataEast= GROUP filterEast BY (state);
characterEast= FOREACH groupDataEast GENERATE group,MAX(filterEast.retweetCount) AS maximumRetweets, AVG(filterEast.friendCount),
AVG(filterEast.followerCount),AVG(filterEast.favoriteCount), AVG(filterEast.statusCount);

--Filtering on Time Zones Mountain, group by state and perform aggregation to determine the character
filterMountain= FILTER joinFollowersAndTweets BY $7>105 AND $7<120;
groupDataMountain= GROUP filterMountain BY (state);
characterMountain= FOREACH groupDataMountain GENERATE group,MAX(filterMountain.retweetCount) AS maximumRetweets, AVG(filterMountain.friendCount),
AVG(filterMountain.followerCount),AVG(filterMountain.favoriteCount), AVG(filterMountain.statusCount);

--Filtering on Time Zones Pacific, group by state and perform aggregation to determine the character
filterPacific= FILTER joinFollowersAndTweets BY $7>120;
groupDataPacific= GROUP filterPacific BY (state);
characterPacific= FOREACH groupDataPacific GENERATE group,MAX(filterPacific.retweetCount) AS maximumRetweets, AVG(filterPacific.friendCount),
AVG(filterPacific.followerCount),AVG(filterPacific.favoriteCount), AVG(filterPacific.statusCount);

-- Store the results in separate output folders 
STORE characterCentral INTO 's3://mrprojectsarika/outputPJF/central' USING PigStorage(',');
STORE characterEast INTO 's3://mrprojectsarika/outputPJF/east' USING PigStorage(',');
STORE characterMountain INTO 's3://mrprojectsarika/outputPJF/mountain' USING PigStorage(',');
STORE characterPacific INTO 's3://mrprojectsarika/outputPJF/pacific' USING PigStorage(',');

