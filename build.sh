export name=taskcenter
export version=latest
echo $(git rev-parse HEAD) > version
echo $(date) > buildtime
docker build -t $name:$version .
