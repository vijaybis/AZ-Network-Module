 cd "C:\Users\pocadmin\Desktop\Project_2\scripts"
 az login --tenant 03e82858-fc14-4f12-b078-aac6d25c87da --service-principal --username de3eddc0-2cbc-4835-a5a1-c0d32bb01ebe --password ULW8Q~U2Xer~jM4DK-LgPpmugusTANu9Xvu.tbgO

.\acr_push.ps1
.\acr_login.ps1

docker images
docker build -t crriskpocdevlab001.azurecr.io/processor-image:v1.0.0 --platform linux/amd64 .

if not tagged
docker tag crriskpocdevlab001.azurecr.io/processor-image:v1.0.0 crriskpocdevlab001.azurecr.io/processor-image:latest

push
docker push crriskpocdevlab001.azurecr.io/batchtask:v1.0.0

verify imgae in ACR
az acr repository list --name crriskpocdevlab001 --output table
az acr repository show-tags --name crriskpocdevlab001 --repository processor-image


Delete Images from ACRF and Docker
az acr repository delete --name crriskpocdevlab001 --repository myapp --yes

docker rmi processor-image:latest 
docker rmi crriskpocdevlab001.azurecr.io/processor-image:v1.0.0
docker rmi crriskpocdevlab001.azurecr.io/processor-image:latest 
docker rmi crriskpocdevlab001.azurecr.io/batchtask:latest






