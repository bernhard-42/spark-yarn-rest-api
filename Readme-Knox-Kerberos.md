## 1 Kerberize cluster

- Use an existing KDC or setup an MIT kdc

- Enable kerberos via Ambari: `http://<Ambari-Server>:8080/#/main/admin/kerberos`.

- Prepare REST APIs for kerberos
	- Follow ["2. Configuring HTTP Authentication for HDFS, YARN, MapReduce2, HBase, Oozie, Falcon and Storm"](http://docs.hortonworks.com/HDPDocuments/Ambari-2.1.2.0/bk_Ambari_Security_Guide/content/_configuring_http_authentication_for_HDFS_YARN_MapReduce2_HBase_Oozie_Falcon_and_Storm.html) to enable kerberos for YARN REST API
	- Set `yarn.resourcemanager.webapp.delegation-token-auth-filter.enabled` to `true` in yarn-site.xml



## 2 Connect Knox to LDAP

- Use existing LDAP Server or start Knox's ldap server (`/usr/hdp/current/knox-server/bin/ldap.sh start`)

- Add a new user to LDAP:

	```bash
	[root@LDAP-HOST ~]$ cat <<user>>.ldif
	dn: uid=<<user>>,ou=people,dc=hadoop,dc=apache,dc=org
	objectclass:top
	objectclass:person
	objectclass:organizationalPerson
	objectclass:inetOrgPerson
	cn: <<user>>
	sn: <<user>>
	uid: <<user>>
	userPassword:<<password>>

	[root@LDAP-HOST ~]$ ldapadd -p 33389  -h localhost  -W -D "uid=admin,ou=people,dc=hadoop,dc=apache,dc=org" -f <<user>>.ldif
	```


## 3 Create a keytab for the user that should run the spark job

- Create keytab for <<user>> for its primary name <<primaryName>>

	```bash
	[root@KDC-HOST ~]$ kadmin
	kadmin> xst -k /etc/security/keytabs/<<primaryName>>.keytab <<primaryName>>@<<REALM>>
	```

-	Copy '/etc/security/keytabs/<<primaryName>>.keytab' to **every** machine on the cluster and set permissions:

	```bash
	[root@CLUSTER-HOST ~]$ chown <<user>>:hadoop /etc/security/keytabs/<<primaryName>>.keytab
	[root@CLUSTER-HOST ~]$ chmod 400 /etc/security/keytabs/<<primaryName>>.keytab
	```

- Test on every machine:

	```bash 
	[root@CLUSTER-HOST ~]$ kinit <<primaryName>>@<<REALM>> -k -t /etc/security/keytabs/<<primaryName>>.keytab
	```

	There must be no password prompt!

	```bash
	[root@KDC-HOST ~]$ klist -l 
	# Principal name                 Cache name
	# --------------                 ----------
	# <<primaryName>>@<<REALM>>      FILE:/tmp/krb5cc_1020
	```



## 4 Test connection from the workstation outside the cluster

- **HDFS** (should work without further configuration)

	```bash
	[MacBook simple-project]$ curl -s -k -u '<<user>>:<<password>>' \
	                          https://$KNOX_SERVER:8443/gateway/default/webhdfs/v1/?op=GETFILESTATUS | jq .
	# {
	#   "FileStatus": {
	#     "accessTime": 0,
	#     "blockSize": 0,
	#     "childrenNum": 9,
	#     "fileId": 16385,
	#     "group": "hdfs",
	#     "length": 0,
	#     "modificationTime": 1458070072105,
	#     "owner": "hdfs",
	#     "pathSuffix": "",
	#     "permission": "755",
	#     "replication": 0,
	#     "storagePolicy": 0,
	#     "type": "DIRECTORY"
	#   }
	# }
	```


- **YARN**

	```bash
	[MacBook simple-project]$ curl -s -k -u '<<user>>:<<password>>' -d '' \
	                          https://$KNOX_SERVER:8443/gateway/default/resourcemanager/v1/cluster/apps/new-application
	# {
	#   "application-id": "application_1460654399208_0004",
	#   "maximum-resource-capability": {
	#     "memory": 8192,
	#     "vCores": 3
	#   }
	# }
	```

## 5 Edit project.cfg

Set at least:

```bash
clusterKerberized = True
hdfsAccessPrincipal = <<primaryName>>@<<REALM>>
hdfsAccessKeytab = /etc/security/keytabs/<<primaryName>>.keytab
```

## 6 Submit job

```bash
[Mac-Book]$ python bin/spark-remote-submit.py
```
