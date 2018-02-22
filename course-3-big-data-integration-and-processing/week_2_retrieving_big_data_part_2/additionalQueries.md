# Additional queries : Postgres and MongoDB #

What is the highest level that the team has reached in gameclicks?

    select max(teamlevel) from gameclicks;

How many userid (repeats allowed) have reached the highest level as found in the previous question?

    select count(userid) from gameclicks where teamlevel = 8;

How many userid (repeats allowed) reached the highest level in game-clicks and also clicked the highest costing price in buy-clicks? 

    select max(price) from buyclicks ; // result = 20
    select count(gameclicks.userId) from gameclicks join buyclicks on buyclicks.userID = gameclicks.userID where teamlevel = 8 and buyclicks.price = 20;


In the MongoDB data set, what is the username of the twitter account who has a **tweet\_followers\_count** of exactly 8973882?

    db.users.find({tweet_followers_count: 8973882}, {user_name: 1}).pretty()
