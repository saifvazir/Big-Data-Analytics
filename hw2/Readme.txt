For task 2, I've used the following command:
spark-submit A2_P2_SparkHT_Vazir_112072061.py 'hdfs:path/to/word_file' 'hdfs://path/to/county_file' --master yarn --deploy-mode cluster

For task 3:
command:
python3 A2_P2_SparkHT_Vazir_112072061.py soc-LiveJournal1.txt

P.S: If you change the dataset, then please remove the line: lines = lines[4:] from the .py file as it's specific to
the soc journal dataset which contains 4 lines of comments.
Thank you!