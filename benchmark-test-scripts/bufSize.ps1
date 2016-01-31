start-process C:\Users\Ian\Documents\GitHub\Fredis.net.exp\fredis-server\bin\Release\fredisServer.exe 512
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -t ping,set,get,incr,mset -n 10000 -q -c 8
stop-process -processname fredisServer

start-process C:\Users\Ian\Documents\GitHub\Fredis.net.exp\fredis-server\bin\Release\fredisServer.exe 1024
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -t ping,set,get,incr,mset -n 10000 -q -c 8
stop-process -processname fredisServer

start-process C:\Users\Ian\Documents\GitHub\Fredis.net.exp\fredis-server\bin\Release\fredisServer.exe 2048
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -t ping,set,get,incr,mset -n 10000 -q -c 8
stop-process -processname fredisServer

start-process C:\Users\Ian\Documents\GitHub\Fredis.net.exp\fredis-server\bin\Release\fredisServer.exe 4096
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -t ping,set,get,incr,mset -n 10000 -q -c 8
stop-process -processname fredisServer

start-process C:\Users\Ian\Documents\GitHub\Fredis.net.exp\fredis-server\bin\Release\fredisServer.exe 8192
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -t ping,set,get,incr,mset -n 10000 -q -c 8
stop-process -processname fredisServer

start-process C:\Users\Ian\Documents\GitHub\Fredis.net.exp\fredis-server\bin\Release\fredisServer.exe 16348
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -t ping,set,get,incr,mset -n 10000 -q -c 8
stop-process -processname fredisServer

start-process C:\Users\Ian\Documents\GitHub\Fredis.net.exp\fredis-server\bin\Release\fredisServer.exe 32768
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -t ping,set,get,incr,mset -n 10000 -q -c 8
stop-process -processname fredisServer

start-process C:\Users\Ian\Documents\GitHub\Fredis.net.exp\fredis-server\bin\Release\fredisServer.exe 65536
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -t ping,set,get,incr,mset -n 10000 -q -c 8
stop-process -processname fredisServer

# .\redis-server .\redis.windows.nosave.conf