#!/bin/bash
inputVertex=""
inputEdge=""
query=""
#benchmark 1000
#uniNumber=1000
#test 1
#uniNumber=1

if [ $# -eq 0 ]
  then
    echo "USAGE: runBenchmark.sh <executorMemory> <executorNumber> <uninumber [1--test, 1000-benchmark]>"
    exit 1;
fi
executeMem=$1  #3g
numExecutor=$2  #4
uniNumber=$3


printf "query,run,time,appId\n"

testeld_sparkql(){
	lubm=$1
	run=$2
	spark-submit --class Main --driver-memory 2g --executor-memory $executeMem  --master spark://ubuntuVM2:7077 --num-executors $numExecutor /home/sparkuser/sparkQl/sparkql/target/scala-2.10/sparkql_2.10-1.0.jar "$inputVertex" "$inputEdge" "$query" 1>result_sparkql_${lubm}_${run}.log 2>error_sparkql_${lubm}_${run}.log

	pid=$(echo $!)
	wait $pid
	applicationId=$(grep EventLoggingListener error_sparkql_${lubm}_${run}.log | cut -d"/" -f8)
	time=$(grep Elapsed result_sparkql_${lubm}_${run}.log | cut -d" " -f3)
	printf "$lubm,$run,$time,$applicationId\n"
}

setParam(){
	queryNum=$1
	
	case "$queryNum" in 
	1)
		#LUBM 1
		query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#GraduateStudent> . ?X <http://spark.elte.hu#takesCourse> <http://www.Department1.University${uniNumber}.edu/GraduateCourse0>"
		;;
	2)
		#LUBM 2
		query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#GraduateStudent> . ?Y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#University> . ?Z <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Department> . ?X <http://spark.elte.hu#memberOf> ?Z . ?Z <http://spark.elte.hu#subOrganizationOf> ?Y . ?X <http://spark.elte.hu#undergraduateDegreeFrom> ?Y"
		;;
	3)
		#LUBM 3
		query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Publication> . ?X <http://spark.elte.hu#publicationAuthor> <http://www.Department1.University${uniNumber}.edu/AssistantProfessor0>"
		;;
	4)
		#LUBM 4
		query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Professor> . ?X <http://spark.elte.hu#worksFor> <http://www.Department1.University${uniNumber}.edu> . ?X <http://spark.elte.hu#name> ?Y1 . ?X <http://spark.elte.hu#emailAddress> ?Y2 . ?X <http://spark.elte.hu#telephone> ?Y3"
		;;
	5)
		#LUBM 5
		query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Person> . ?X <http://spark.elte.hu#memberOf> <http://www.Department1.University${uniNumber}.edu>"
		;;
	6)
		#LUBM 6
		query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Student>"
		;;
	7)
		#LUBM 7
		query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Student> . ?Y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Course> . ?X <http://spark.elte.hu#takesCourse> ?Y . <http://www.Department1.University${uniNumber}.edu/AssociateProfessor0> <http://spark.elte.hu#teacherOf> ?Y"
		;;
	8)
		#LUBM 8
		query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Student> . ?Y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Department> . ?X <http://spark.elte.hu#memberOf> ?Y . ?Y <http://spark.elte.hu#subOrganizationOf> <http://www.University${uniNumber}.edu> . ?X <http://spark.elte.hu#emailAddress> ?Z"
		;;
	9)
		#LUBM 9
		query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Student> . ?Y <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Faculty> . ?Z <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Course> . ?X <http://spark.elte.hu#advisor> ?Y . ?Y <http://spark.elte.hu#teacherOf> ?Z . ?X <http://spark.elte.hu#takesCourse> ?Z"
		;;
	10)
		#LUBM 10
		query="?X <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://spark.elte.hu#Student> . ?X <http://spark.elte.hu#takesCourse> <http://www.Department1.University${uniNumber}.edu/GraduateCourse0>"
		;;
	*)
		exit 1
		;;
	esac
}

#SparkQl
for num in 20 #10 20 40 80 
do
echo "Sparkql LUBM_$num"
#read N3
inputVertex="/home/selyeuser/s2x/inputs/LUBM${num}.n3"
#read converted graph
#inputVertex="/home/selyeuser/sparkQl/benchmarkInput/vertex${num}/*"
#inputEdge="/home/selyeuser/sparkQl/benchmarkInput/edge${num}/*"

for lubm_i in 1 3 4 5 6 9 10 #$(seq 1 10)	#1 10
do
	setParam $lubm_i
	for run_i in $(seq 0 4)
	do
               #Start worker
               (cd /home/selyeuser/spark/spark-1.5.2-bin-hadoop2.6/bin && ./spark-class org.apache.spark.deploy.worker.Worker spark://ubuntuVM2:7077 -c 20 -m 40g &)
               sleep 30

		testeld_sparkql $lubm_i $run_i

               #kill worker
               pid=$(ps aux | grep worker.Worker | grep -v grep | sed -e 's/\s\s*/ /g' | cut -d" " -f2 | head -n 1)
               kill $pid
               sleep 30

	done
done

done
