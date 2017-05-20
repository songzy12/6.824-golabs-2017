rm log
for i in {1..1}
do
    #go test -run 2A -v 
    #go test -run 2B -v
    #go test -run 2C -v

    #go test -run ConcurrentStarts2B -v
    go test -run Backup2B -v
    #go test -run Persist22C -v
    #go test -run TestFigure82C -v
    #go test -run TestFigure8Unreliable2C -v
    #go test -run TestUnreliableChurn2C -v
done
