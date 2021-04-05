#!/bin/bash

numofexp=$1

home=`pwd`
result_dir=$home/$2

mkdir -p $result_dir

dev="/dev/nvme1n1"
threads="16"
key_size="16"
value_size="1000 4000" 
dataset_size="1000000000000"
# dataset_size="10000000000"

#tests="eval_scan_64"
tests="eval_scan"
index_type="2" #rocks
pack_thres="2000"
prefetch_ena="TRUE FALSE"
for exp_id in $( seq 1 $numofexp )
do
	for k_len in $key_size
	do
		for v_len in $value_size
		do
			for testfile in $tests
			do
				for index in $index_type
				do
					#export INDEX_TYPE=$index

					result_txt=$result_dir/${testfile}_${index}_${exp_id}_${k_len}_${v_len}
					# clean file if existed
					echo "" > $result_txt
					for numofthreads in $threads
					do
						echo ===== $numofthreads threads ====== >> $result_txt
						echo "" >> $result_txt

						if [ "$v_len" == "1000" ]; then
							dev="/dev/nvme2n1"
						elif [ "$v_len" == "4000" ]; then
							dev="/dev/nvme5n1"
						else
							dev="/dev/nvme2n1"
						fi

						# format kvssd
						nvme format $dev
						echo "format $dev success"

						recordcnt=$(printf %d $(echo "$dataset_size/$v_len" | bc))
						sed -i 's/recordcount=.*/recordcount='$recordcnt'/' workloads/$testfile
						sed -i 's/zeropadding=.*/zeropadding='$k_len'/' workloads/$testfile

						sed -i 's/fieldlength=.*/fieldlength='$v_len'/' workloads/$testfile
						sed -i 's/minfieldlength=.*/minfieldlength='$v_len'/' workloads/$testfile
						
						sed -i '/dev_name/c\   \"dev_name\" : \"'$dev'\",' kvrangedb_config.json
						sed -i '/index_type/c\   \"index_type\" : \'$index'\,' kvrangedb_config.json
						sed -i '/pack_size_thres/c\   \"pack_size_thres\" : \'$pack_thres'\,' kvrangedb_config.json

						# ycsb load
						/usr/bin/time -v  ./bin/ycsb.sh load kvrangedb -s -P workloads/$testfile -threads $numofthreads 1>out.log 2> err.log 
						echo $testfile results >> $result_txt
						echo load >> $result_txt
						printf "load_tp: " >> $result_txt
						cat out.log|grep OVERALL|grep Throughput|awk '{print $3}' >> $result_txt
						printf "\n" >> $result_txt
						printf "load_lat_95th: " >> $result_txt
						grep "INSERT" out.log | grep "95thPercentileLatency" | awk '{print $3}' >> $result_txt
						printf "\n" >> $result_txt
						printf "load_lat_99th: " >> $result_txt
						grep "INSERT" out.log | grep "99thPercentileLatency" | awk '{print $3}' >> $result_txt

						# copy log file
						cp out.log $result_dir/load_out_${index}_${exp_id}_${k_len}_${v_len}.log
						cp err.log $result_dir/load_err_${index}_${exp_id}_${k_len}_${v_len}.log

						# report cpu
						printf "cpu_user: " >> $result_txt
						cat err.log | grep "User time" | awk '{print $4}' >> $result_txt
						printf "cpu_system: " >> $result_txt
						cat err.log | grep "System time" | awk '{print $4}' >> $result_txt

						# report io

						# warmup
						# sed -i 's/operationcount=.*/operationcount=10000000/' workloads/$testfile
									
						# for prefetch in $prefetch_ena
						# do
						# 	export PREFETCH_ENA=$prefetch
						# 	echo "run with prefetch_ena=$prefetch" >> $result_txt

						# 	sleep 3
						# 	# ycsb run scan 100
						# 	sed -i 's/maxscanlength.*/maxscanlength=100/' workloads/$testfile
						# 	sed -i 's/minscanlength.*/minscanlength=100/' workloads/$testfile
						# 	# num queries 
						# 	if [ "$index" == "BASE" ]; then
						# 		sed -i 's/operationcount=.*/operationcount=100/' workloads/$testfile
						# 	else
						# 		sed -i 's/operationcount=.*/operationcount=10000/' workloads/$testfile
						# 	fi

						# 	./bin/ycsb.sh run kvrangedb -s -P workloads/$testfile -threads $numofthreads > out.log  
						# 	echo "run scan 100" >> $result_txt
						# 	printf "run_tp: " >> $result_txt
						# 	cat out.log|grep OVERALL|grep Throughput|awk '{print $3}' >> $result_txt
						# 	printf "scan_lat: " >> $result_txt
						# 	cat out.log|grep AverageLatency|grep SCAN|awk '{print $3}' >> $result_txt
						# 	printf "scan_lat_99th: " >> $result_txt
						# 	cat out.log|grep "SCAN" |grep 99thPercentileLatency |awk '{print $3}' >> $result_txt
							
						# 	echo "" >> $result_txt
						# 	# report io
						# 	printf "store_ios: " >> $result_txt
						# 	cat kv_device.log|grep ", get"| awk '{ SUM += $2} END { print SUM }' >> $result_txt
						# 	printf "append_ios: " >> $result_txt
						# 	cat kv_device.log|grep ", get"| awk '{ SUM += $4} END { print SUM }' >> $result_txt
						# 	printf "get_ios: " >> $result_txt
						# 	cat kv_device.log|grep ", get"| awk '{ SUM += $6} END { print SUM }' >> $result_txt
						# 	printf "delete_ios: " >> $result_txt
						# 	cat kv_device.log|grep ", get"| awk '{ SUM += $8} END { print SUM }' >> $result_txt
						# 	rm out.log

						# 	echo "" >> $result_txt
						# 	sleep 3
							
						# done

						# # ycsb run scan 1 (seek)
						# sed -i 's/maxscanlength.*/maxscanlength=1/' workloads/$testfile
						# sed -i 's/minscanlength.*/minscanlength=1/' workloads/$testfile
						# # num queries 
						# if [ "$index" == "BASE" ]; then
						# 	sed -i 's/operationcount=.*/operationcount=100/' workloads/$testfile
						# else
						# 	sed -i 's/operationcount=.*/operationcount=100000/' workloads/$testfile
						# fi

						# ./bin/ycsb.sh run kvrangedb -s -P workloads/$testfile -threads $numofthreads > out.log  
						# echo "run scan 1" >> $result_txt
						# printf "run_tp: " >> $result_txt
						# cat out.log|grep OVERALL|grep Throughput|awk '{print $3}' >> $result_txt
						# printf "scan_lat: " >> $result_txt
						# cat out.log|grep AverageLatency|grep SCAN|awk '{print $3}' >> $result_txt
						# printf "scan_lat_99th: " >> $result_txt
						# cat out.log|grep "SCAN" |grep 99thPercentileLatency |awk '{print $3}' >> $result_txt

						# echo "" >> $result_txt
						# # report io
						# printf "store_ios: " >> $result_txt
						# cat kv_device.log|grep ", get"| awk '{ SUM += $2} END { print SUM }' >> $result_txt
						# printf "append_ios: " >> $result_txt
						# cat kv_device.log|grep ", get"| awk '{ SUM += $4} END { print SUM }' >> $result_txt
						# printf "get_ios: " >> $result_txt
						# cat kv_device.log|grep ", get"| awk '{ SUM += $6} END { print SUM }' >> $result_txt
						# printf "delete_ios: " >> $result_txt
						# cat kv_device.log|grep ", get"| awk '{ SUM += $8} END { print SUM }' >> $result_txt
						# rm out.log
							
						# echo "" >> $result_txt

						rm -rf *.log # clean up files
					done
				done
			done
		done
	done
done

echo testing completed
