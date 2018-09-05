#!/bin/bash
yes | rm query_runner_db.zip 
7z a query_runner_db.zip *.py ./dep/* dbclient
