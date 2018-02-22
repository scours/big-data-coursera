# Querying documents in MongoDB #

1.	Start MongoDB Server and Shell
2.	Show databases and collections
3.	Look at twitter document and find distinct values for a field
4.	Search for specific field value
5.	Filter fields returned in a query


## Start MongoDB Server and Shell ##

Start the MongoDB server with the argument **--dbpath**, which specifies the directory to use for datafiles (**db** in our example). Then, start the MongoDB shell to query the server

    ./mongodb/bin/mongod --dbpath db
    ./mongodb/bin/mongo


## Show databases and collections ##

Show the databases and switch to the database named **sample**

    show dbs
    use sample

Show the colletions in the **sample** database and count the number of documents for **users** collection

    show collections
    db.users.count()


## Look at twitter document and find distinct values for a field ##

Examine the content of one document

    db.users.findOne()

The document has several fields, e.g., user_name, retweet_count, tweet_ID, etc., and nested fields under user, e.g., CreatedAt, UserId, Location, etc.

Let's find the distinct values for **user\_name**

    db.users.distinct("user_name")




## Search for specific field value ##

We can search for fields with a specific value, let's search for **user\_name** with the value **"ActionSportsJax"**. Then, format the result with **.pretty** function

    db.users.find({user_name : "ActionSportsJax"}).pretty()




## Filter fields returned in a query ##

We can use the previous query and print only **tweet_ID** field. Remove also the **\_id** field, which is the primary key for every document

    db.users.find({user_name : "ActionSportsJax"}, {tweet_ID: 1, _id: 0}).pretty()

Perform regular expresion search : let's search for the value **FIFA** in the **tweet\_text** field

    db.users.find({tweet_text: /FIFA/})

Search using text index to speed up searches and allow advanced searches with **$text**. Let's first create the index on the field **tweet\_text**

    db.users.createIndex({"tweet_text" : "text"})

Now, we can use the **$text** operator to search for documents containing **FIFA** (the first query) or documents containing **FIFA**, but not **Texas** (second query)

    db.users.find({$text : {$search : "FIFA"}}).count()
    db.users.find({$text : {$search : "FIFA - Texas"}}).count()

Search using operators

Let's search documents where the **tweet\_mentioned\_count** field is greater than 6

    db.users.find({tweet_mentioned_count: {$gt: 6}})

Now, let's use the **$where** command to compare fields within the same document. For example, we can search for the number of documents where **tweet\_mentioned\_count** is greater than **tweet\_followers\_count**

    db.users.find({$where : "this.tweet_mentioned_count > this.tweet_followers_count"}).count()

We can combine multiple searches by using **$and** command. For example, let's search for the number of documents where **tweet\_text** contains **FIFA** and **tweet\_mentioned\_count** is greater than four

    db.users.find({$and : [{tweet_text : /FIFA/}, {tweet_mentioned_count : {$gt: 4}}]}).count()