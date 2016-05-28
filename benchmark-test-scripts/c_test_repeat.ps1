


echo "warm up"
redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 1
echo "warm up complete"

$NumRepeats = 16

for ($i=1; $i -le $NumRepeats; $i++)
{
    redis-cli flushdb
    echo "redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 1"    
    redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 1
}

for ($i=1; $i -le $NumRepeats; $i++)
{
    redis-cli flushdb
    echo "redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 2"
    redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 2
}

for ($i=1; $i -le $NumRepeats; $i++)
{
    redis-cli flushdb
    echo "redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 4"
    redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 4
}

for ($i=1; $i -le $NumRepeats; $i++)
{
    redis-cli flushdb
    echo "redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 8"
    redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 8
}

for ($i=1; $i -le $NumRepeats; $i++)
{
    redis-cli flushdb
    echo "redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 16"
    redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 16
}

for ($i=1; $i -le $NumRepeats; $i++)
{
    redis-cli flushdb
    echo "redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 32"
    redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 32
}

for ($i=1; $i -le $NumRepeats; $i++)
{
    redis-cli flushdb
    echo "redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 64"
    redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 64
}

for ($i=1; $i -le $NumRepeats; $i++)
{
    redis-cli flushdb
    echo "redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 128"
    redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 128
}

for ($i=1; $i -le $NumRepeats; $i++)
{
    redis-cli flushdb
    echo "redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 256"
    redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 256
}

for ($i=1; $i -le $NumRepeats; $i++)
{
    redis-cli flushdb
    echo "redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 512"
    redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 512
}

for ($i=1; $i -le $NumRepeats; $i++)
{
    redis-cli flushdb
    echo "redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 1024"
    redis-benchmark -r 64 -d 1024 --csv -t ping,set,get,incr,mset -n 100000 -q -c 1024
}


