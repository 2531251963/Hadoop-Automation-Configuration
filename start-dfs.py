import os
os.putenv('JAVA_HOME','/home/lyh/jdk1.8')
os.putenv('HADOOP_HOME','/home/lyh/hadoop')
os.putenv('PATH',os.getenv('PATH')+':/home/lyh/jdk1.8/bin:/home/lyh/hadoop/sbin:/home/lyh/hadoop/bin')
os.system('java -version')
os.system('echo $PATH')
os.system('echo $HADOOP_HOME')
os.system('hadoop version')
os.system('~/hadoop/sbin/start-dfs.sh')
