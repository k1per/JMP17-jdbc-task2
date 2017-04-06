gradlew bootRun

-----------------------------

1. Create simple database with tables Users (id, name, surname, birthdate), Friendships (userid1, userid2, timestamp), Posts(id, userId, text, timestamp), Likes (postid, userid, timestamp). 
2. Fill tables with data which are make sense (> 1 000 users, > 70 000 friendships, > 300 000 likes in 2015). 
3. Program should print out all names (only distinct) of users who has more when 100 friends and 100 likes in March 2015. All actions (table creation, insert data and reading) should be implemented with JDBC.