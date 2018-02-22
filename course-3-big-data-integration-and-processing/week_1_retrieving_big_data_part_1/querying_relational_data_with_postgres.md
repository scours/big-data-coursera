# Querying relational data with Postgres #
1. View table and column definitions, and perform SQL queries in the Postgres shell
2. Query the contents of SQL tables
3. Filter table rows and columns
4. Combine two tables by joining them on a column


Start Postgres shell and list the tables in the database

    psql
    \d

Show the column definitions of a specific table (**buyclicks** in our example). Then, show **buyclicks** content

    \d buyclicks
	select * from buyclicks;

### Filter rows and columns ###

Query only the price and userid

    select price, userid from buyclicks;

Query rows with a price greater than 10

    select price, userid from buyclicks where price > 10;

Perform aggregate operations, such as average and sum

    select avg(price) from buyclicks;
    select sum(price) from buyclicks;

### Combine two tables ###

Combine the contents of two tables by matching or joining them on a single column (**userid** in our example)

    select adid, buyid, adclicks.userid
    from adclicks join buyclicks on adclicks.userid = buyclicks.userid;