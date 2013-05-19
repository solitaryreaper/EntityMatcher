echo "Cleaning up existing hadoop directories .."
hadoop dfs -rmr /user/hduser/entitymatcher-out

echo "Compiling the java classes .."
cd /home/hduser/workspace/EntityMatcher/src/hdpjobs

javac -classpath /home/hduser/hadoop/hadoop-core-0.20.1.jar -d ../../classes/ SetCartesianProduct.java 

echo "Creating the jar .."
jar -cvf entitymatcher.jar -C ../../classes/ .

echo "Launching the hadoop job .."
hadoop jar entitymatcher.jar hdpjobs.SetCartesianProduct /user/hduser/entitymatcher /user/hduser/entitymatcher-out 
