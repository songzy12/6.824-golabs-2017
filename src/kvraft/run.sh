rm log
for i in {1..1}
do
    go test -run ConcurrentPartition -v
done
