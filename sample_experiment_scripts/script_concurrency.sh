#! /bin/bash

for (( i = 1 ; i <= 12 ; i++ ))  do
      java -jar CityBench.jar rate=1.0 frequency=1.0 duration=15m queryDuplicates=1 startDate=2014-08-11T11:00:00 endDate=2014-08-31T11:00:00 engine=cqels query=Q$i.txt

done
for (( i = 1 ; i <= 12 ; i++ ))  do
      java -jar CityBench.jar rate=1.0 frequency=1.0 duration=15m queryDuplicates=20 startDate=2014-08-11T11:00:00 endDate=2014-08-31T11:00:00 engine=cqels query=Q$i.txt

done
for (( i = 1 ; i <= 12 ; i++ ))  do
      java -jar CityBench.jar rate=1.0 frequency=1.0 duration=15m queryDuplicates=50 startDate=2014-08-11T11:00:00 endDate=2014-08-31T11:00:00 engine=cqels query=Q$i.txt

done

for (( i = 1 ; i <= 12 ; i++ ))  do
      java -jar CityBench.jar rate=1.0 frequency=1.0 duration=15m queryDuplicates=1 startDate=2014-08-11T11:00:00 endDate=2014-08-31T11:00:00 engine=csparql query=Q$i.txt

done
for (( i = 1 ; i <= 12 ; i++ ))  do
      java -jar CityBench.jar rate=1.0 frequency=1.0 duration=15m queryDuplicates=20 startDate=2014-08-11T11:00:00 endDate=2014-08-31T11:00:00 engine=csparql query=Q$i.txt

done
for (( i = 1 ; i <= 12 ; i++ ))  do
      java -jar CityBench.jar rate=1.0 frequency=1.0 duration=15m queryDuplicates=50 startDate=2014-08-11T11:00:00 endDate=2014-08-31T11:00:00 engine=csparql query=Q$i.txt

done
