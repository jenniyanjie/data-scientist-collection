# this script contains functions:
# hiveConnection: to connect hive server
# redshiftConnection: to connect to amazon redshift
# mySQLConnection: to connect mySQL database

hiveConnection <- function(hiveStr){
	# this function is to connecting hive server using RJDBC
	# input: 
	#    hiveStr: the query string
	# output:
	#    dt: query result in data.table 
    start_time<-Sys.time()
    library(rJava)
    library(RJDBC)
    library(data.table)
	
	Sys.setenv(JAVA_HOME = 'C:\\Program Files\\Java\\jre1.8.0_91') # copy the location of java_home here
	options( java.parameters = "-Xmx8g" ) # set the max memory the java can use
	
	# Since the JDBC driver depends on some Hadoop dependencies and for convenience in the following 
	# part we are going to load all jars, including Hadoop common, into R’s classpath
	cp = c("C:\\opt\\lib\\hive-jdbc.jar", "C:\\opt\\lib\\hadoop-common.jar",
		   "C:\\opt\\lib\\libthrift-0.9.2.jar","C:\\opt\\lib\\hive-service-1.1.0-cdh5.5.0.jar",
		   "C:\\opt\\lib\\httpclient-4.2.5.jar", "C:\\opt\\lib\\httpcore-4.2.5.jar",
		   "C:\\opt\\lib\\hive-jdbc-1.1.0-cdh5.5.0-standalone.jar")
	.jinit(classpath = cp) # load the .jar file for JDBC driver

    #initial the connection
    drv <- JDBC("org.apache.hive.jdbc.HiveDriver",
                "C:\\opt\\lib\\hive-jdbc.jar",
                identifier.quote = "`")
    conn <- dbConnect(drv, "jdbc:hive2://servername:10000/dbname", "username", "password")
    dt <- data.table(dbGetQuery(conn, hiveStr))
    dbDisconnect(conn)
    cat ('complete query!\n')
    cat ('time usage: ',round(difftime(Sys.time(),start_time,units = "mins"),3),'mins\n')
    return(dt)
}

redshiftConnection <- function(redshiftSql) {
	# this function is to connecting Amazon redshift server using RJDBC
	# input: 
	#    redshiftsql: the query string
	# output:
	#    dt: query result in data.table 
	start_time <- Sys.time()
    library(rJava)
    library(RJDBC)
    library(data.table)
    
    Sys.setenv(JAVA_HOME='C:\\Program Files\\Java\\jre1.8.0_91') # copy the location of java_home here
    options( java.parameters = "-Xmx8g" ) # set the max memory the java can use
    
    # connect to Amazon Redshift
    driver <- JDBC("com.amazon.redshift.jdbc42.Driver", "C:\\opt\\lib\\RedshiftJDBC42-1.1.17.1017.jar", identifier.quote="`")
	
    # url <- "<JDBCURL>:<PORT>/<DBNAME>?user=<USER>&password=<PW>
    url <- "jdbc:redshift://redshift-dwh-cluster.cxrqbo0xwuno.ap-southeast-1.redshift.amazonaws.com:5439/aoc_dwh?user=jyan&password=RfzwFJx2"
    conn <- dbConnect(driver, url)
    dt <- data.table(dbGetQuery(conn, redshiftSql))
    dbDisconnect(conn)
    cat ('complete query!\n')
    cat ('time usage: ',round(difftime(Sys.time(),start_time,units = "mins"),3),'mins\n')
    return(dt)
}

mySQLConnection <- function (sqlStr) {
	# this function is to connecting mySQL using RMySQL
	# input: 
	#    sqlStr: the query string
	# output:
	#    dt: query result in data.table 
	start_time <- Sys.time()
    library(RMySQL)
	library(data.table)
    mydb <- dbConnect(MySQL(), user ='username', password = 'password',
                    dbname = 'dbname', host = 'host')
    dt <- data.table(fetch(dbSendQuery(mydb,sqlStr),-1))
    dbDisconnect(mydb)
	cat ('complete query!\n')
    cat ('time usage: ',round(difftime(Sys.time(),start_time,units = "mins"),3),'mins\n')
    return(dt)
}