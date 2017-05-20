rm log
for i in {1..1}
do
    go test -run TestFigure82C -v
    #go test -run TestFigure8Unreliable2C -v
    #go test -run TestUnreliableChurn2C -v
done
