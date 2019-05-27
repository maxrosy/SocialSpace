#!/bin/bash

mongo --host 172.16.42.2:27017 -u logrotate -p logrotate --authenticationDatabase admin admin /home/panther/SocialSpace/jobs/mongo/logRotate.js
mongo --host 172.16.42.3:27017 -u logrotate -p logrotate --authenticationDatabase admin admin /home/panther/SocialSpace/jobs/mongo/logRotate.js
