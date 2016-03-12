C:\ProgramData\chocolatey\lib\redis-64\redis-cli flushdb
echo "redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 1"
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 1

C:\ProgramData\chocolatey\lib\redis-64\redis-cli flushdb
echo "redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 2"
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 2

C:\ProgramData\chocolatey\lib\redis-64\redis-cli flushdb
echo "redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 4"
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 4

C:\ProgramData\chocolatey\lib\redis-64\redis-cli flushdb
echo "redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 8"
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 8

C:\ProgramData\chocolatey\lib\redis-64\redis-cli flushdb
echo "redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 16"
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 16

C:\ProgramData\chocolatey\lib\redis-64\redis-cli flushdb
echo "redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 32"
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 32

C:\ProgramData\chocolatey\lib\redis-64\redis-cli flushdb
echo "redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 64"
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 64

C:\ProgramData\chocolatey\lib\redis-64\redis-cli flushdb
echo "redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 128"
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 128

C:\ProgramData\chocolatey\lib\redis-64\redis-cli flushdb
echo "redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 256"
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 256

C:\ProgramData\chocolatey\lib\redis-64\redis-cli flushdb
echo "redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 512"
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 512

C:\ProgramData\chocolatey\lib\redis-64\redis-cli flushdb
echo "redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 1024"
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -t ping,set,get,incr,mset -n 10000 -d 10000 -q -c 1024



# .\redis-server .\redis.windows.nosave.conf