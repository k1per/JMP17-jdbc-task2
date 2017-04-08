package com.epam.learning.aykorenev.jdbc.task2

import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import java.sql.Connection
import java.sql.Date
import java.sql.DriverManager
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.util.*
import java.util.concurrent.ThreadLocalRandom


/**
 * Created by Anton_Korenev on 4/4/2017.
 */
@SpringBootApplication
class Application : CommandLineRunner {

    val connection: Connection = DriverManager.getConnection("jdbc:hsqldb:mem:mytest")

    override fun run(vararg args: String?) {

        connection.use { _ ->
            createDDL()
            printCreatedTables()
            insertData()
            printTOP100AllUsers()
            printTop100Friendships()
            printTop100Posts()
            printTop100Likes()
            printHomeWorkSolution()
        }
    }

    private fun printHomeWorkSolution() {

        val createStatement = connection.createStatement()
        createStatement.use { createStatement ->

            println("Printing users with 100 friends")
            val selectFriendMoreThan100 = """SELECT
                         userid1,
                         count(userid2) as total
                        FROM Friendships
                        GROUP BY userid1
                        HAVING count(userid2) >= 10"""
            var resultSet = createStatement.executeQuery(selectFriendMoreThan100)
            try {
                while (resultSet.next()) {
                    println("user id : ${resultSet.getString("userid1")} , count : ${resultSet.getInt("total")}")
                }

                println("Printing users with more than 100 likes in March 2015")
                val selectMoreThan100Likes = """
            SELECT
            userId,
            count(*) as total
            FROM Likes
            WHERE year(likedTime) = 2015 AND month(likedTime) = 3
            GROUP BY userId
            HAVING count(*) >= 100
            """
                resultSet = createStatement.executeQuery(selectMoreThan100Likes)
                while (resultSet.next()) {
                    println("user id : ${resultSet.getString("userId")} , count : ${resultSet.getInt("total")}")
                }
                println("Getting distinct users with 100 friend and 100 likes in March 2015")
                val select = """SELECT DISTINCT USERS.name
                        FROM USERS AS users
                        JOIN
                        (SELECT
                         userid1,
                         count(userid2)
                        FROM Friendships
                        GROUP BY userid1
                        HAVING count(userid2) >= 100) AS friends
                        ON users.id = friends.userid1
                        JOIN (SELECT
                              userId,
                              count(*)
                            FROM Likes
                            WHERE year(likedTime) = 2015 AND month(likedTime) = 3
                            GROUP BY userId
                            HAVING count(*) >= 100) AS likes ON users.id = likes.userId;
        """
                resultSet = createStatement.executeQuery(select)
                while (resultSet.next()) {
                    println(resultSet.getString("name"))
                }
            } finally {
                resultSet?.close()
            }
        }
    }


    private fun printTop100Likes() {
        val statement = connection.createStatement()
        statement.use { statement ->
            {
                val rs = statement.executeQuery("SELECT * FROM Likes LIMIT 100")
                rs.use { rs ->
                    {
                        while (rs.next()) {
                            print("post id: ${rs.getInt("postId")} ")
                            print("user id: ${rs.getInt("userId")} ")
                            print("timestap: ${rs.getTimestamp("likedTime")}")
                            println()
                        }
                    }
                }
            }
        }
    }


    private fun printTop100Posts() {
        val statement = connection.createStatement()
        statement.use { statement ->
            {
                val rs = statement.executeQuery("SELECT * FROM Posts LIMIT 100")
                rs.use { rs ->
                    {
                        while (rs.next()) {
                            print("userId : ${rs.getString("userid")} ")
                            print("text : ${rs.getString("text")} ")
                            print("published time : ${rs.getTimestamp("publishedTime")} ")
                            println()
                        }
                    }
                }
            }
        }
    }


    private fun printTop100Friendships() {
        val statement = connection.createStatement()
        statement.use { statement ->
            val rs = statement.executeQuery("SELECT * FROM Friendships LIMIT 100")
            rs.use { rs ->
                {
                    while (rs.next()) {
                        print(rs.getString("userid1") + " ")
                        print(rs.getString("userid2") + " ")
                        print(rs.getTimestamp("since"))
                        println()
                    }
                }
            }
        }
    }

    private fun printTOP100AllUsers() {
        val statement = connection.createStatement()
        statement.use { statement ->
            val rs = statement.executeQuery("SELECT * FROM Users LIMIT 100")
            rs.use { rs ->
                while (rs.next()) {
                    print(rs.getString("id") + " ")
                    print(rs.getString("name") + " ")
                    print(rs.getString("surname") + " ")
                    print(rs.getDate("birthday").toLocalDate())
                    println()
                }
            }
        }
    }

    private fun insertData() {
        insertUsers()
        insertFriendships()
        insertPost()
        insertLikes()
    }

    private fun insertLikes() {
        val ps = connection.prepareStatement("INSERT INTO Likes VALUES (?, ?, ?)")
        ps.use { ps ->
            for (i in 1..MAX_LIKES) {
                val postID = ThreadLocalRandom.current().nextInt(10_000)
                val userId = ThreadLocalRandom.current().nextInt(1000)
                ps.setInt(1, postID)
                ps.setInt(2, userId)
                ps.setTimestamp(3, getRandomTimeStampBetweenDates(LocalDateTime.of(2015, 1, 1, 10, 0, 0), LocalDateTime.of(2016, 6, 6, 23, 59, 59)))
                ps.addBatch()
                if (i % 10_000 == 0) {
                    ps.executeBatch()
                }
            }
        }
    }

    private fun insertPost() {
        val ps = connection.prepareStatement("INSERT INTO Posts VALUES (?, ?, ?, ?)")
        ps.use { ps ->
            for (i in 1..MAX_POSTS) {
                val userid = ThreadLocalRandom.current().nextInt(1, 70000)
                val post = UUID.randomUUID().toString()
                ps.setInt(1, i)
                ps.setInt(2, userid)
                ps.setString(3, post)
                ps.setTimestamp(4, getRandomTimeStampBetweenDates(LocalDateTime.of(2013, 1, 1, 10, 0, 0), LocalDateTime.now()))
                ps.addBatch()
            }
            ps.executeBatch()
        }
    }

    private fun insertFriendships() {

        val ps = connection.prepareStatement("INSERT INTO Friendships VALUES (?, ?, ?)")
        ps.use { ps ->
            for (i in 1..MAX_FRIENDSHIPS) {
                val userid = ThreadLocalRandom.current().nextInt(1, 1000)
                val userid2 = ThreadLocalRandom.current().nextInt(1, 1000)
                val friendsSince = getRandomTimeStampBetweenDates(LocalDateTime.of(2010, 1, 1, 10, 0, 0), LocalDateTime.now())
                ps.setInt(1, userid)
                ps.setInt(2, userid2)
                ps.setTimestamp(3, friendsSince)
                ps.addBatch()
                if (i % 10000 == 0) {
                    ps.executeBatch()
                }
            }
        }
    }

    private fun getRandomTimeStampBetweenDates(start: LocalDateTime, end: LocalDateTime): Timestamp {
        val startDate = start.toLocalDate().toEpochDay()
        val startTime = start.toLocalTime().toNanoOfDay()

        val endDate = end.toLocalDate().toEpochDay()
        val endTime = end.toLocalTime().toNanoOfDay()

        val randomDate = ThreadLocalRandom.current().nextLong(startDate, endDate)
        val randomTime = ThreadLocalRandom.current().nextLong(startTime, endTime)
        val randomDateTime = LocalDateTime.of(LocalDate.ofEpochDay(randomDate), LocalTime.ofNanoOfDay(randomTime))
        return Timestamp.valueOf(randomDateTime)
    }

    private fun insertUsers() {
        val ps = connection.prepareStatement("INSERT INTO USERS VALUES (?, ?, ?, ?)")
        ps.use { ps ->
            for (i in 1..MAX_USERS) {
                ps.setInt(1, i)
                ps.setString(2, getRandomName())
                ps.setString(3, "Surname$i")
                ps.setDate(4, randomDateBetween(LocalDate.of(1990, 1, 1).toEpochDay(), LocalDate.now().toEpochDay()))
                ps.addBatch()
            }
            ps.executeBatch()
        }
    }

    private fun getRandomName() = listOfNames[ThreadLocalRandom.current().nextInt(listOfNames.size)]

    private fun randomDateBetween(start: Long, end: Long): Date {
        val randomDate = ThreadLocalRandom.current().nextLong(start, end)
        return Date.valueOf(LocalDate.ofEpochDay(randomDate))
    }

    private fun printCreatedTables() {
        val tables = connection.metaData.getTables(null, null, null, arrayOf("TABLE"))
        tables.use { tables ->
            while (tables.next()) {
                println(tables.getString(3))
            }
        }
    }

    private fun createDDL() {
        val statement = connection.createStatement()
        statement.use { statement ->
            println("Creating tables")
            statement.execute("CREATE TABLE USERS (id INT, name VARCHAR(255),surname VARCHAR(255), birthday DATE);")
            statement.execute("CREATE TABLE Friendships (userid1 INT, userid2 INT, since Timestamp);")
            statement.execute("CREATE TABLE Posts (id INT, userId INT , text VARCHAR(2000), publishedTime TimeStamp);")
            statement.execute("CREATE TABLE Likes (postId INT, userId INT , likedTime TIMESTAMP);")
        }
    }

    companion object {
        val MAX_USERS = 5000
        val MAX_FRIENDSHIPS = 140_000
        val MAX_POSTS = 500_000
        val MAX_LIKES = 1_500_000
        val listOfNames = listOf("Firelord", "Bilbo", "Anton", "Hellboy",
                "Johny", "Peter", "Igor", "Yuriy", "Alisa", "Mary", "Charlie", "Michael")
    }
}