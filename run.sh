#!/bin/bash

CQLSH=/home/cassandra/cassandra/bin/cqlsh
CASSANDRA_HOST=127.0.0.1
CASSANDRA_PORT=8042
DROP='DROP KEYSPACE IF EXISTS titan;'

ROOT_DIR=.
CONF_FILE=$ROOT_DIR/src/test/resources/META-INF/input.properties

dataset() {
    if [ $1 -eq 1 ]; then
        data="email-Enron.txt"
    elif [ $1 -eq 2 ]; then
        data="amazon0601.txt"
    elif [ $1 -eq 3 ]; then
        data="com-youtube.ungraph.txt"
    elif [ $1 -eq 4 ]; then
        data="com-lj.ungraph.txt"
    elif [ $1 -eq 5 ]; then
        data="network1000.dat"
    elif [ $1 -eq 6 ]; then
        data="network5000.dat"
    elif [ $1 -eq 7 ]; then
        data="network10000.dat"
    elif [ $1 -eq 8 ]; then
        data="network20000.dat"
    elif [ $1 -eq 9 ]; then
        data="network30000.dat"
    elif [ $1 -eq 10 ]; then
        data="network40000.dat"
    elif [ $1 -eq 11 ]; then
        data="network50000.dat"
    else
        echo "Wrong test number: $i, must be 1,2,3...11"
        exit 1
    fi
    prefix=`echo $data | awk -F'.' '{print $1}'`
}

community() {
    if [ $1 -eq 5 ]; then
        community="community1000.dat"
    elif [ $1 -eq 6 ]; then
        community="community5000.dat"
    elif [ $1 -eq 7 ]; then
        community="community10000.dat"
    elif [ $1 -eq 8 ]; then
        community="community20000.dat"
    elif [ $1 -eq 9 ]; then
        community="community30000.dat"
    elif [ $1 -eq 10 ]; then
        community="community40000.dat"
    elif [ $1 -eq 11 ]; then
        community="community50000.dat"
    else
        echo "Wrong test number: $i, must be 5,6,7...11"
        exit 1
    fi
}

run() {
    # modify configuration
    sed -i "s/eu.socialsensor.results-path=results/eu.socialsensor.results-path=results\/$prefix/g" $CONF_FILE
    sed -i "s/^#eu.socialsensor.benchmarks=MASSIVE_INSERTION/eu.socialsensor.benchmarks=MASSIVE_INSERTION/g" $CONF_FILE
    if [ $1 -lt 5 ]; then
        sed -i "s/^#eu.socialsensor.dataset=data\/$data/eu.socialsensor.dataset=data\/$data/g" $CONF_FILE
        sed -i "s/^#eu.socialsensor.benchmarks=FIND_NEIGHBOURS/eu.socialsensor.benchmarks=FIND_NEIGHBOURS/g" $CONF_FILE
        sed -i "s/^#eu.socialsensor.benchmarks=FIND_ADJACENT_NODES/eu.socialsensor.benchmarks=FIND_ADJACENT_NODES/g" $CONF_FILE
        sed -i "s/^#eu.socialsensor.benchmarks=FIND_SHORTEST_PATH/eu.socialsensor.benchmarks=FIND_SHORTEST_PATH/g" $CONF_FILE
    else
        community $1
        sed -i "s/^#eu.socialsensor.dataset=data\/network1000.dat/eu.socialsensor.dataset=data\/$data/g" $CONF_FILE
        sed -i "s/^#eu.socialsensor.actual-communities=data\/community1000.dat/eu.socialsensor.actual-communities=data\/$community/g" $CONF_FILE
        sed -i "s/^#eu.socialsensor.benchmarks=CLUSTERING/eu.socialsensor.benchmarks=CLUSTERING/g" $CONF_FILE
    fi
    # clear data of neo4j and hugegraphcore
    if [ -d $ROOT_DIR/storage ]; then
        rm -fr $ROOT_DIR/storage
    fi
    # clear data of titan in cassandra
    $CQLSH $CASSANDRA_HOST $CASSANDRA_PORT -e "$DROP"
    # clear results
    if [ -d $ROOT_DIR/results/$prefix ]; then
        rm -fr $ROOT_DIR/results/$prefix
    fi
    # test
    mvn test > logs/$prefix.log 2>&1
    if [ $? -eq 0 ]; then
        echo "$prefix executed successfully!"
    else
        echo "$prefix executed failed!"
    fi
    # restore configuration
    sed -i "s/^eu.socialsensor.dataset=data\/$data/#eu.socialsensor.dataset=data\/$data/g" $CONF_FILE
    sed -i "s/^eu.socialsensor.results-path=results\/$prefix/eu.socialsensor.results-path=results/g" $CONF_FILE
}

run_test() {
    # backup configuration file
    cp $CONF_FILE $CONF_FILE.bak
    sed -i "s/^eu.socialsensor.dataset/#eu.socialsensor.dataset/g" $CONF_FILE
    sed -i "s/^eu.socialsensor.benchmarks/#eu.socialsensor.benchmarks/g" $CONF_FILE
    dataset $1
    echo "Starting run test: $data"
    run $1
    # resume configuration file
    rm -f $CONF_FILE
    mv $CONF_FILE.bak $CONF_FILE
}

if [ $# -eq 0 -o "$1" = "-h" ]; then
    echo "Usage: ./run.sh test-number"
    echo "For example:"
    echo "  ./run.sh 1"
    echo "  ./run.sh 1 2 3"
    echo "  ./run.sh all"
    echo ""
    echo "test-number represent different tests:"
    echo "1     email-enron"
    echo "2     amazon"
    echo "3     youtube"
    echo "4     com-lj"
    echo "5     network1000"
    echo "6     network5000"
    echo "7     network10000"
    echo "8     network20000"
    echo "9     network30000"
    echo "10    network40000"
    echo "11    network50000"
    echo "all   represent all test_number that can pass currently"
    echo ""
    echo "Currently tests with test-number [1,2,3,5,6,7,8] can pass"
    echo "Test 4 will cause OOM when using titan and hugegraphcore"
    echo "and tests [9,10,11,12] will cost more than 3 hours when using titan"
    exit 1
fi

if [ $# -eq 1 -a "$1" = "all" ]; then
    tests="1 2 3 5 6 7 8"
    echo "Starting run all tests..."
    for i in $tests;
    do
        echo $i
        run_test $i
    done
else
    for i in $@;
    do
        run_test $i
    done
fi
