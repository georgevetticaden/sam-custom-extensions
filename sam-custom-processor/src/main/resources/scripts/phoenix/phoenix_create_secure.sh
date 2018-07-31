Save Password
G
Save this password in Okta?
Save Password Never for this App
Disable for all apps
#!/bin/bash

#TODO: Phoenix version dependency is baked into paths here.

# Note if installed by the script the path will be: /root/phoenix-4.1.0-bin/hadoop2/bin/psql.py'
# Otherwise it's /usr/hdp/current/phoenix-client/bin/psql.py
# So let's test and use the one we have!
if [ -f /usr/hdp/current/phoenix-client/bin/psql.py ]
 	then
 		psql_script=/usr/hdp/current/phoenix-client/bin/psql.py
    	echo "** Using psql script found at: /usr/hdp/current/phoenix-client/bin"

	elif [ -f /root/phoenix-4.1.0-bin/hadoop2/bin/psql.py ]
	    then
		    psql_script=/root/phoenix-4.1.0-bin/hadoop2/bin/psql.py
		    echo "** Using psql script found at: /root/phoenix-4.1.0-bin/hadoop2/bin/psql.py"
    else
        echo "** Error: psql script could not be found. Unable to create phoenix tables! **"
fi


# Run the scripts
$psql_script <ZK_HOST>:2181:/hbase-secure:$PRINCIPAL@REALM:$KEYTAB_LOCATION ./sql/phoenix_create_drivers.sql data/drivers.csv
$psql_script <ZK_HOST>:2181:/hbase-secure:$PRINCIPAL@REALM:$KEYTAB_LOCATION ./sql/phoenix_create_timesheet.sql data/timesheet.csv