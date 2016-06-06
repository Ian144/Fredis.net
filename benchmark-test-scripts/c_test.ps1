
redis-cli flushdb
echo "redis-benchmark -r 64 -d 1024 -t ping,set,get,incr,mset --csv -n 10000 -q -c 1"
redis-benchmark -r 64 -d 1024 -t ping,set,get,incr,mset --csv -n 10000 -q -c 1

redis-cli flushdb
echo "redis-benchmark -r 64 -d 1024 -t ping,set,get,incr,mset --csv -n 10000 -q -c 2"
redis-benchmark -r 64 -d 1024 -t ping,set,get,incr,mset --csv -n 10000 -q -c 2

redis-cli flushdb
echo "redis-benchmark -r 64 -d 1024 -t ping,set,get,incr,mset --csv -n 10000 -q -c 4"
redis-benchmark -r 64 -d 1024 -t ping,set,get,incr,mset --csv -n 10000 -q -c 4

redis-cli flushdb
echo "redis-benchmark -r 64 -d 1024 -t ping,set,get,incr,mset --csv -n 10000 -q -c 8"
redis-benchmark -r 64 -d 1024 -t ping,set,get,incr,mset --csv -n 10000 -q -c 8

redis-cli flushdb
echo "redis-benchmark -r 64 -d 1024 -t ping,set,get,incr,mset --csv -n 10000 -q -c 16"
redis-benchmark -r 64 -d 1024 -t ping,set,get,incr,mset --csv -n 10000 -q -c 16

redis-cli flushdb
echo "redis-benchmark -r 64 -d 1024 -t ping,set,get,incr,mset --csv -n 10000 -q -c 32"
redis-benchmark -r 64 -d 1024 -t ping,set,get,incr,mset --csv -n 10000 -q -c 32

redis-cli flushdb
echo "redis-benchmark -r 64 -d 1024 -t ping,set,get,incr,mset --csv -n 10000 -q -c 64"
redis-benchmark -r 64 -d 1024 -t ping,set,get,incr,mset --csv -n 10000 -q -c 64

redis-cli flushdb
echo "redis-benchmark -r 64 -d 1024 -t ping,set,get,incr,mset --csv -n 10000 -q -c 128"
redis-benchmark -r 64 -d 1024 -t ping,set,get,incr,mset --csv -n 10000 -q -c 128

redis-cli flushdb
echo "redis-benchmark -r 64 -d 1024 -t ping,set,get,incr,mset --csv -n 10000 -q -c 256"
redis-benchmark -r 64 -d 1024 -t ping,set,get,incr,mset --csv -n 10000 -q -c 256

redis-cli flushdb
echo "redis-benchmark -r 64 -d 1024 -t ping,set,get,incr,mset --csv -n 10000 -q -c 512"
redis-benchmark -r 64 -d 1024 -t ping,set,get,incr,mset --csv -n 10000 -q -c 512

redis-cli flushdb
echo "redis-benchmark -r 64 -d 1024 -t ping,set,get,incr,mset --csv -n 10000 -q -c 1024"
redis-benchmark -r 64 -d 1024 -t ping,set,get,incr,mset --csv -n 10000 -q -c 1024
