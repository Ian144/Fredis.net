C:\ProgramData\chocolatey\lib\redis-64\redis-cli flushdb
echo redis-benchmark -d 1       
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -d 1       -t ping,set,get,incr,mset -n 10000 -q -c 8

C:\ProgramData\chocolatey\lib\redis-64\redis-cli flushdb
echo redis-benchmark -d 10      
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -d 10      -t ping,set,get,incr,mset -n 10000 -q -c 8

C:\ProgramData\chocolatey\lib\redis-64\redis-cli flushdb
echo redis-benchmark -d 100     
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -d 100     -t ping,set,get,incr,mset -n 10000 -q -c 8

C:\ProgramData\chocolatey\lib\redis-64\redis-cli flushdb
echo redis-benchmark -d 1000    
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -d 1000    -t ping,set,get,incr,mset -n 10000 -q -c 8

C:\ProgramData\chocolatey\lib\redis-64\redis-cli flushdb
echo redis-benchmark -d 10000   
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -d 10000   -t ping,set,get,incr,mset -n 10000 -q -c 8

C:\ProgramData\chocolatey\lib\redis-64\redis-cli flushdb
echo redis-benchmark -d 100000  
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -d 100000  -t ping,set,get,incr,mset -n 10000 -q -c 8

C:\ProgramData\chocolatey\lib\redis-64\redis-cli flushdb
echo redis-benchmark -d 1000000 
C:\ProgramData\chocolatey\lib\redis-64\redis-benchmark -d 1000000 -t ping,set,get,incr,mset -n 10000 -q -c 8

