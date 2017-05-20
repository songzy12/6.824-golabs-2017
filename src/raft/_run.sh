rm log
for i in {1..1}
do
    go test -run 2A -v
    go test -run 2B -v
    go test -run Persist -v
    go test -run 2C -v
done
