# `BigData` Практична робота №3.

## Завдання
Реалізувати множення двох матриць з використанням `MapReduce` алгоритму.

## Виконання
### Алгоритм
Алгоритм я не змыг придумати самотужки, тому скористався [статтею](https://medium.com/analytics-vidhya/matrix-multiplication-at-scale-using-map-reduce-d5dc16710095) на ресурсі Medium.

### Код
Весь необхідний код задач знаходиться у файлі [MatrixMul.java](/src/main/java/MatrixMul.java).

### Результат
Тестування роботоспроможності алгоритму виконувалося на наступному прикладі множення матриць:


![картинка](https://drive.google.com/uc?export=view&id=1WsHj6NhWMhPF94FC9AkPNJpokPStOSW3)

Вхідні данні:
```bash
➜  docker-hadoop git:(master) ✗ docker exec -it namenode /bin/bash
root@fd03c76925f9:/# ls
KEYS  bin  boot  dev  entrypoint.sh  etc  hadoop  hadoop-data  home  lab_3  lib  lib64  media  mnt  opt  proc  root  run  run.sh  sbin  srv  sys  tmp  usr  var
root@fd03c76925f9:/# cd lab_3/
root@fd03c76925f9:/lab_3# cat M-matrix.txt 
0,0,2.0
0,1,1.0
1,0,-3.0
2,0,4.0
2,1,-1.0
root@fd03c76925f9:/lab_3# cat N-matrix.txt 
0,0,5.0
0,1,-1.0
0,2,6.0
1,0,-3.0
1,2,7.0
root@fd03c76925f9:/lab_3# hdfs dfs -mkdir -p /user/root
root@fd03c76925f9:/lab_3# hdfs dfs -put M-matrix.txt /user/root/input
2022-11-20 21:07:34,847 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
root@fd03c76925f9:/lab_3# hdfs dfs -put N-matrix.txt /user/root/input
2022-11-20 21:07:44,502 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
```

Запускаємо алгоритм:
```bash
root@fd03c76925f9:/lab_3# hadoop jar BigDataLab3.jar input/M-matrix.txt input/N-matrix.txt intermediate output
2022-11-20 21:11:54,642 INFO client.RMProxy: Connecting to ResourceManager at resourcemanager/10.254.30.5:8032
2022-11-20 21:11:54,778 INFO client.AHSProxy: Connecting to Application History server at historyserver/10.254.30.3:10200
Exception in thread "main" org.apache.hadoop.mapred.FileAlreadyExistsException: Output directory hdfs://namenode:9000/user/root/intermediate already exists
        at org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.checkOutputSpecs(FileOutputFormat.java:164)
        at org.apache.hadoop.mapreduce.JobSubmitter.checkSpecs(JobSubmitter.java:277)
        at org.apache.hadoop.mapreduce.JobSubmitter.submitJobInternal(JobSubmitter.java:143)
        at org.apache.hadoop.mapreduce.Job$11.run(Job.java:1570)
        at org.apache.hadoop.mapreduce.Job$11.run(Job.java:1567)
        at java.security.AccessController.doPrivileged(Native Method)
        at javax.security.auth.Subject.doAs(Subject.java:422)
        at org.apache.hadoop.security.UserGroupInformation.doAs(UserGroupInformation.java:1730)
        at org.apache.hadoop.mapreduce.Job.submit(Job.java:1567)
        at org.apache.hadoop.mapreduce.Job.waitForCompletion(Job.java:1588)
        at MatrixMul.main(MatrixMul.java:243)
        at sun.reflect.NativeMethodAccessorImpl.invoke0(Native Method)
        at sun.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
        at sun.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
        at java.lang.reflect.Method.invoke(Method.java:498)
        at org.apache.hadoop.util.RunJar.run(RunJar.java:323)
        at org.apache.hadoop.util.RunJar.main(RunJar.java:236)
root@fd03c76925f9:/lab_3# hdfs dfs -rm -r /user/root/intermediate
Deleted /user/root/intermediate
root@fd03c76925f9:/lab_3# hdfs dfs -rm -r /user/root/output      
rm: `/user/root/output': No such file or directory
root@fd03c76925f9:/lab_3# hadoop jar BigDataLab3.jar input/M-matrix.txt input/N-matrix.txt intermediate output
2022-11-20 21:12:31,803 INFO client.RMProxy: Connecting to ResourceManager at resourcemanager/10.254.30.5:8032
2022-11-20 21:12:31,935 INFO client.AHSProxy: Connecting to Application History server at historyserver/10.254.30.3:10200
2022-11-20 21:12:32,060 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
2022-11-20 21:12:32,076 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/root/.staging/job_1668978211341_0002
2022-11-20 21:12:32,152 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
2022-11-20 21:12:32,326 INFO input.FileInputFormat: Total input files to process : 1
2022-11-20 21:12:32,343 INFO input.FileInputFormat: Total input files to process : 1
2022-11-20 21:12:32,373 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
2022-11-20 21:12:32,390 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
2022-11-20 21:12:32,394 INFO mapreduce.JobSubmitter: number of splits:2
2022-11-20 21:12:32,529 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
2022-11-20 21:12:32,954 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1668978211341_0002
2022-11-20 21:12:32,955 INFO mapreduce.JobSubmitter: Executing with tokens: []
2022-11-20 21:12:33,091 INFO conf.Configuration: resource-types.xml not found
2022-11-20 21:12:33,091 INFO resource.ResourceUtils: Unable to find 'resource-types.xml'.
2022-11-20 21:12:33,341 INFO impl.YarnClientImpl: Submitted application application_1668978211341_0002
2022-11-20 21:12:33,370 INFO mapreduce.Job: The url to track the job: http://resourcemanager:8088/proxy/application_1668978211341_0002/
2022-11-20 21:12:33,370 INFO mapreduce.Job: Running job: job_1668978211341_0002
2022-11-20 21:12:39,439 INFO mapreduce.Job: Job job_1668978211341_0002 running in uber mode : false
2022-11-20 21:12:39,441 INFO mapreduce.Job:  map 0% reduce 0%
2022-11-20 21:12:44,485 INFO mapreduce.Job:  map 100% reduce 0%
2022-11-20 21:12:47,502 INFO mapreduce.Job:  map 100% reduce 100%
2022-11-20 21:12:48,523 INFO mapreduce.Job: Job job_1668978211341_0002 completed successfully
2022-11-20 21:12:48,585 INFO mapreduce.Job: Counters: 54
        File System Counters
                FILE: Number of bytes read=83
                FILE: Number of bytes written=689696
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=556
                HDFS: Number of bytes written=129
                HDFS: Number of read operations=11
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters 
                Launched map tasks=2
                Launched reduce tasks=1
                Rack-local map tasks=2
                Total time spent by all maps in occupied slots (ms)=13196
                Total time spent by all reduces in occupied slots (ms)=13024
                Total time spent by all map tasks (ms)=3299
                Total time spent by all reduce tasks (ms)=1628
                Total vcore-milliseconds taken by all map tasks=3299
                Total vcore-milliseconds taken by all reduce tasks=1628
                Total megabyte-milliseconds taken by all map tasks=13512704
                Total megabyte-milliseconds taken by all reduce tasks=13336576
        Map-Reduce Framework
                Map input records=10
                Map output records=10
                Map output bytes=200
                Map output materialized bytes=97
                Input split bytes=474
                Combine input records=0
                Combine output records=0
                Reduce input groups=2
                Reduce shuffle bytes=97
                Reduce input records=10
                Reduce output records=13
                Spilled Records=20
                Shuffled Maps =2
                Failed Shuffles=0
                Merged Map outputs=2
                GC time elapsed (ms)=100
                CPU time spent (ms)=1030
                Physical memory (bytes) snapshot=976293888
                Virtual memory (bytes) snapshot=18674446336
                Total committed heap usage (bytes)=1003487232
                Peak Map Physical memory (bytes)=362258432
                Peak Map Virtual memory (bytes)=5109948416
                Peak Reduce Physical memory (bytes)=255148032
                Peak Reduce Virtual memory (bytes)=8455307264
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters 
                Bytes Read=0
        File Output Format Counters 
                Bytes Written=129
2022-11-20 21:12:48,612 INFO client.RMProxy: Connecting to ResourceManager at resourcemanager/10.254.30.5:8032
2022-11-20 21:12:48,612 INFO client.AHSProxy: Connecting to Application History server at historyserver/10.254.30.3:10200
2022-11-20 21:12:48,618 WARN mapreduce.JobResourceUploader: Hadoop command-line option parsing not performed. Implement the Tool interface and execute your application with ToolRunner to remedy this.
2022-11-20 21:12:48,626 INFO mapreduce.JobResourceUploader: Disabling Erasure Coding for path: /tmp/hadoop-yarn/staging/root/.staging/job_1668978211341_0003
2022-11-20 21:12:48,634 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
2022-11-20 21:12:49,179 INFO input.FileInputFormat: Total input files to process : 1
2022-11-20 21:12:49,192 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
2022-11-20 21:12:49,617 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
2022-11-20 21:12:50,037 INFO mapreduce.JobSubmitter: number of splits:1
2022-11-20 21:12:50,063 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
2022-11-20 21:12:50,077 INFO mapreduce.JobSubmitter: Submitting tokens for job: job_1668978211341_0003
2022-11-20 21:12:50,077 INFO mapreduce.JobSubmitter: Executing with tokens: []
2022-11-20 21:12:50,306 INFO impl.YarnClientImpl: Submitted application application_1668978211341_0003
2022-11-20 21:12:50,319 INFO mapreduce.Job: The url to track the job: http://resourcemanager:8088/proxy/application_1668978211341_0003/
2022-11-20 21:12:50,319 INFO mapreduce.Job: Running job: job_1668978211341_0003
2022-11-20 21:13:00,426 INFO mapreduce.Job: Job job_1668978211341_0003 running in uber mode : false
2022-11-20 21:13:00,426 INFO mapreduce.Job:  map 0% reduce 0%
2022-11-20 21:13:05,462 INFO mapreduce.Job:  map 100% reduce 0%
2022-11-20 21:13:08,479 INFO mapreduce.Job:  map 100% reduce 100%
2022-11-20 21:13:09,500 INFO mapreduce.Job: Job job_1668978211341_0003 completed successfully
2022-11-20 21:13:09,526 INFO mapreduce.Job: Counters: 54
        File System Counters
                FILE: Number of bytes read=90
                FILE: Number of bytes written=459391
                FILE: Number of read operations=0
                FILE: Number of large read operations=0
                FILE: Number of write operations=0
                HDFS: Number of bytes read=250
                HDFS: Number of bytes written=90
                HDFS: Number of read operations=8
                HDFS: Number of large read operations=0
                HDFS: Number of write operations=2
                HDFS: Number of bytes read erasure-coded=0
        Job Counters 
                Launched map tasks=1
                Launched reduce tasks=1
                Rack-local map tasks=1
                Total time spent by all maps in occupied slots (ms)=6332
                Total time spent by all reduces in occupied slots (ms)=12728
                Total time spent by all map tasks (ms)=1583
                Total time spent by all reduce tasks (ms)=1591
                Total vcore-milliseconds taken by all map tasks=1583
                Total vcore-milliseconds taken by all reduce tasks=1591
                Total megabyte-milliseconds taken by all map tasks=6483968
                Total megabyte-milliseconds taken by all reduce tasks=13033472
        Map-Reduce Framework
                Map input records=13
                Map output records=13
                Map output bytes=208
                Map output materialized bytes=82
                Input split bytes=121
                Combine input records=0
                Combine output records=0
                Reduce input groups=9
                Reduce shuffle bytes=82
                Reduce input records=13
                Reduce output records=9
                Spilled Records=26
                Shuffled Maps =1
                Failed Shuffles=0
                Merged Map outputs=1
                GC time elapsed (ms)=66
                CPU time spent (ms)=680
                Physical memory (bytes) snapshot=616566784
                Virtual memory (bytes) snapshot=13570629632
                Total committed heap usage (bytes)=682098688
                Peak Map Physical memory (bytes)=358690816
                Peak Map Virtual memory (bytes)=5109755904
                Peak Reduce Physical memory (bytes)=257875968
                Peak Reduce Virtual memory (bytes)=8460873728
        Shuffle Errors
                BAD_ID=0
                CONNECTION=0
                IO_ERROR=0
                WRONG_LENGTH=0
                WRONG_MAP=0
                WRONG_REDUCE=0
        File Input Format Counters 
                Bytes Read=129
        File Output Format Counters 
                Bytes Written=90
```

Вихідні дані:
```bash
root@fd03c76925f9:/lab_3# hdfs dfs -cat /user/root/output/*
2022-11-20 21:13:55,411 INFO sasl.SaslDataTransferClient: SASL encryption trust check: localHostTrusted = false, remoteHostTrusted = false
0,0,    7.0
0,1,    -2.0
0,2,    19.0
1,0,    -15.0
1,1,    3.0
1,2,    -18.0
2,0,    23.0
2,1,    -4.0
2,2,    17.0
```

## Висновки
Виконуючи цю практичну роботу, я навчився писати задачі для проєкту `Hadoop`. Було реаліщовано множення двох матриць з використанням `MapReduce` алгоритму.
