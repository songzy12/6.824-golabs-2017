export GOPATH=$PWD"/../..";
echo $GOPATH
mkdir temp/
for i in `seq 0 9`
do
   touch ./temp/res$i
   #go test &
   go test > ./temp/res$i &
done
