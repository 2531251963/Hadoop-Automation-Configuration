# coding=utf-8
import os
import xml.dom.minidom
import subprocess
import socket
import sys
from subprocess import call,Popen
username=os.getenv('USER')#lyh
hostname=socket.gethostname()#lyh1
print(username)
print(hostname)
iscopyjdk=sys.argv[len(sys.argv)-1]
slaves=[]
for inx, val in enumerate(sys.argv):
    if inx>0:
        slaves.append(val)

for i in slaves:
    print(i)
del slaves[len(slaves)-1]
os.system('tar -zxvf hadoop.tar.gz')
os.system('cd ~/hadoop \n mkdir -m 755 hadoopmrsys \n mkdir -m 755 hadoopmrlocal \n mkdir -m 755 nodemanagerlocal \n mkdir -m 755 nodemanagerlogs \n mkdir -m 755 nodemanagerremote')
#os.system('sudo chmod -R 777 hadoop')

core_site_xml_name = ["fs.defaultFS", "fs.default.name"]
core_site_xml_value = ["hdfs://"+hostname+":9000", "hdfs://"+hostname+":9000"]
core_site_xml_str = '<?xml version="1.0" encoding="UTF-8"?><?xml-stylesheet type="text/xsl" href="configuration.xsl"?><configuration>'
for i in range(len(core_site_xml_name)):
    core_site_xml_str+='<property><name>'+core_site_xml_name[i]+'</name><value>'+core_site_xml_value[i]+'</value></property>'
core_site_xml_str+='</configuration>'
os.system("echo '"+core_site_xml_str+"'> /home/"+username+"/hadoop/etc/hadoop/core-site.xml")


hdfs_site_xml_name = ["dfs.namenode.secondary.http-address","dfs.namenode.name.dir","dfs.datanode.data.dir","hadoop.tmp.dir","dfs.replication","dfs.permissions","dfs.webhdfs.enabled","dfs.support.append","hadoop.proxyuser.hadoop.hosts","hadoop.proxyuser.hadoop.groups","dfs.ha.fencing.methods","dfs.ha.fencing.ssh.private-key-files"]
hdfs_site_xml_value = ["lyh1:9001", "/home/"+username+"/hadoop/namedir","/home/"+username+"/hadoop/datadir","/home/"+username+"/hadoop/tmp","2","false","true","true","*","*","sshfence","/home/"+username+"/.ssh/id_rsa"]
print(len(hdfs_site_xml_name))
print(len(hdfs_site_xml_value))
hdfs_site_xml_str ='<?xml version="1.0" encoding="UTF-8"?><?xml-stylesheet type="text/xsl" href="configuration.xsl"?><configuration>'
for i in range(len(hdfs_site_xml_name)):
    hdfs_site_xml_str+='<property><name>'+hdfs_site_xml_name[i]+'</name><value>'+hdfs_site_xml_value[i]+'</value></property>'
hdfs_site_xml_str+='</configuration>'
os.system("echo '"+hdfs_site_xml_str+"'> /home/"+username+"/hadoop/etc/hadoop/hdfs-site.xml")

mapred_site_xml_name = ["mapreduce.framework.name","mapreduce.jobhistory.address","mapred.job.tracker","mapreduce.jobhistory.webapp.address","mapred.system.dir","mapred.local.dir"]
mapred_site_xml_value = ["yarn", ""+hostname+":10020",""+hostname+":54311",""+hostname+":19888","/home/"+username+"/hadoop/hadoopmrsys","/home/"+username+"/hadoop/hadoopmrlocal"]
print(len(mapred_site_xml_name))
print(len(mapred_site_xml_value))
mapred_site_xml_str = '<?xml version="1.0" encoding="UTF-8"?><?xml-stylesheet type="text/xsl" href="configuration.xsl"?><configuration>'
for i in range(len(mapred_site_xml_name)):
    mapred_site_xml_str+='<property><name>'+mapred_site_xml_name[i]+'</name><value>'+mapred_site_xml_value[i]+'</value>'
    if mapred_site_xml_name[i]=='mapred.system.dir' or mapred_site_xml_name[i]=='mapred.local.dir':
        mapred_site_xml_str+='<final>true</final>'
    mapred_site_xml_str+='</property>'
mapred_site_xml_str+='</configuration>'
os.system('cp /home/'+username+'/hadoop/etc/hadoop/mapred-site.xml.template /home/'+username+'/hadoop/etc/hadoop/mapred-site.xml')
os.system("echo '"+mapred_site_xml_str+"'> /home/"+username+"/hadoop/etc/hadoop/mapred-site.xml")

yarn_site_xml_name = ["yarn.nodemanager.aux-services","yarn.nodemanager.aux-services.mapreduce.shuffle.class","yarn.nodemanager.local-dirs","yarn.nodemanager.remote-app-log-dir","mapreduce.jobhistory.webapp.address","yarn.resourcemanager.address","yarn.resourcemanager.scheduler.address","yarn.resourcemanager.resource-tracker.address","yarn.resourcemanager.admin.address","yarn.resourcemanager.webapp.address","yarn.nodemanager.resource.memory-mb"]
yarn_site_xml_value = ["mapreduce_shuffle", "org.apache.hadoop.mapred.ShuffleHandler","/home/"+username+"/hadoop/nodemanagerlocal","/home/"+username+"/hadoop/nodemanagerlogs","/home/"+username+"/hadoop/nodemanagerremote",""+hostname+":8032",""+hostname+":8030",""+hostname+":8031",""+hostname+":8033",""+hostname+":18088","2048"]
print(len(yarn_site_xml_name))
print(len(yarn_site_xml_value))
yarn_site_xml_str = '<?xml version="1.0" encoding="UTF-8"?><?xml-stylesheet type="text/xsl" href="configuration.xsl"?><configuration>'
for i in range(len(yarn_site_xml_name)):
    yarn_site_xml_str+='<property><name>'+yarn_site_xml_name[i]+'</name><value>'+yarn_site_xml_value[i]+'</value></property>'
yarn_site_xml_str+='</configuration>'
os.system("echo '"+yarn_site_xml_str+"'> /home/"+username+"/hadoop/etc/hadoop/yarn-site.xml")
slavesstr=''
for i in range(0,len(slaves),1):
    slavesstr+=slaves[i]+"\n"
os.system('echo "'+slavesstr+'" > /home/'+username+'/hadoop/etc/hadoop/slaves')
os.system('echo "export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native" >> /home/'+username+'/hadoop/etc/hadoop/hadoop-env.sh')
os.system('echo "export JAVA_HOME=/home/'+username+'/jdk1.8" >> /home/'+username+'/hadoop/etc/hadoop/hadoop-env.sh')
print('########################################################################################################')
print('########################################自动配置成功#####################################################')
print('########################################################################################################')
print('########################################即将并行拷贝文件#################################################')
print('########################################################################################################')
#os.system('sudo sed -i "1i 192.168.56.2     lyh1" /etc/hosts')
#os.system('scp -r ~/hadoop lyh@192.168.56.3:~')
for i in range(0,len(slaves),1):
    print(slaves[i])
    if slaves[i]!=hostname:
        p=subprocess.Popen('scp -r ~/hadoop '+username+'@'+slaves[i]+':~', shell=True)
        if iscopyjdk=='yes':
            os.system('scp -r ~/jdk1.8 '+username+'@'+slaves[i]+':~')
            print('wait.................................................')
        p.wait()
print('########################################################################################################')
print('########################################拷贝文件成功####################################################')
print('########################################################################################################')
os.putenv('JAVA_HOME','/home/'+username+'/jdk1.8')
os.putenv('HADOOP_HOME','/home/'+username+'/hadoop')
os.putenv('PATH',os.getenv('PATH')+':/home/'+username+'/jdk1.8/bin:/home/'+username+'/hadoop/sbin:/home/'+username+'/hadoop/bin')
os.system('java -version')
os.system('echo $PATH')
os.system('echo $HADOOP_HOME')
os.system('hadoop version')
print('########################################################################################################')
print('########################################即将格式化#######################################################')
print('########################################################################################################')
print('the status code is:',p.returncode)
os.system('hdfs namenode -format')
print('########################################################################################################')
print('########################################即将启动Hadoop集群###############################################')
print('########################################################################################################')
os.system('start-dfs.sh')


