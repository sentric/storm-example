# storm-example

# Topologies

## Twitter client count

Start locally with `mvn`:

    mvn exec:java -Dexec.classpathScope=compile -Dexec.mainClass=storm.starter.TwitterClientCountTopology -Dexec.args="$twitter_user $twitter_password"

It requires a running redis locally and it writes its data into a Sorted Set named `clients` on DB2. You can change that in `TwitterClientCountTopology`.

Redis installation:
	brew install redis

To start redis manually:
    redis-server /usr/local/etc/redis.conf

