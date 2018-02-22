# Additional MongoDB queries #

How many tweets have location not null (null type number is 10)?

    db.users.find({"user.Location" : {$type : 10}}).count()

How many people have more followers than friends?

    db.users.find({$where : "this.user.FollowersCount > this.user.FriendsCount"}).count()

Return text of tweets which have the string "http://" ?

    db.users.find({tweet_text : /http:\/\//}, {tweet_text : 1}).pretty()

Return all the tweets which contain text **"England"** but not **"UEFA"** 

    db.users.find({$text : {$search : "England -UEFA"}}).pretty()

Get all the tweets from the location **"Ireland"** and which contain the string **"UEFA"**

    db.users.find({$and : [{ $text : {$search : "UEFA"}}, {"user.Location" : "Ireland"}]}).pretty()

Export **tweet\_text** field into a csv file, using **sample** database and **users** collection

    ./mongodb/bin/mongoexport --db sample --collection users --out tweetsData.csv --fields tweet_text --type=csv
