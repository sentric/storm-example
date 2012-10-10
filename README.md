# storm-example

# Topologies

## Twitter client count

Start locally with `mvn`:

    mvn exec:java -Dexec.classpathScope=compile -Dexec.mainClass=ch.sentric.storm.example.topology.TwitterClientCountTopology -Dexec.args="$twitter_user $twitter_password"

It requires a running redis locally and it writes its data into a Sorted Set named `clients` on DB2. You can change that in `TwitterClientCountTopology`.

Redis installation:
	brew install redis

To start redis manually:
    redis-server /usr/local/etc/redis.conf
    
Connect to database:    
	redis-cli -n 2
	
List top 10 clients:
	ZREVRANGE clients 0 10 WITHSCORES

