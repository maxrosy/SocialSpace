#!/bin/bash

mongo --host localhost:27017 -u logrotate -p logrotate --authenticationDatabase admin admin /home/panther/SocialSpace/jobs/mongo/logRotate.js
