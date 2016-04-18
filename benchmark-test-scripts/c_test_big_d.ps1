redis-cli flushdb
echo "redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 1"
redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 1

redis-cli flushdb
echo "redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 2"
redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 2

redis-cli flushdb
echo "redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 4"
redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 4

redis-cli flushdb
echo "redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 8"
redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 8

redis-cli flushdb
echo "redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 16"
redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 16

redis-cli flushdb
echo "redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 32"
redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 32

redis-cli flushdb
echo "redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 64"
redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 64

redis-cli flushdb
echo "redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 128"
redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 128

redis-cli flushdb
echo "redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 256"
redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 256

redis-cli flushdb
echo "redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 512"
redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 512

redis-cli flushdb
echo "redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 1024"
redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 1024



# .\redis-server .\redis.windows.nosave.conf