 cd "C:\Users\pocadmin\Desktop\Project_2\scripts"
 az login --tenant 03e82858-fc14-4f12-b078-aac6d25c87da --service-principal --username de3eddc0-2cbc-4835-a5a1-c0d32bb01ebe --password ULW8Q~U2Xer~jM4DK-LgPpmugusTANu9Xvu.tbgO


#Storage account
az storage account create --name storageriskpoc003 --resource-group rg-risk-poc-batch-devlab-002 --location switzerlandnorth --sku Standard_LRS --kind StorageV2 --default-action Deny --allow-blob-public-access false --https-only true --min-tls-version TLS1_2 --require-infrastructure-encryption true --allow-shared-key-access false --public-network-access Disabled --allow-cross-tenant-replication false 

az network private-endpoint create  --name pep-blob-sariskpoc-devlab-003 --resource-group  rg-risk-poc-batch-devlab-002 --vnet-name vnet-risk-poc-devlab-001 --subnet snet-batch-devlab-001E --private-connection-resource-id $(az storage account show --name storageriskpoc003 --resource-group rg-risk-poc-batch-devlab-002 --query "id" --output tsv) --group-id blob --connection-name "storageriskpoc003-connection"

az storage container create --name application --account-name sariskpocdevlab001 --auth-mode login

az storage container list --account-name sariskpocdevlab001 --output table --auth-mode login
az storage account list --query "[].{Name:name}" --output table

az storage blob upload-batch --account-name application --destination batchtask-app --source ./bin/Release/net8.0/win-x64/publish --auth-mode login
az storage blob upload-batch --account-name sariskpocdevlab001 --destination application --source ./publish --auth-mode login

az storage blob delete-batch --account-name sariskpocdevlab001 --source application --auth-mode login
az storage container delete --name application --account-name sariskpocdevlab001 --auth-mode login

az storage blob upload --account-name sariskpocdevlab001 --container-name input-container --name input.txt --auth-mode login

az storage container list --account-name sariskpocdevlab001 --query "[].{Name:name}" --output table --auth-mode login

# Verify upload
az storage blob list --account-name sariskpocdevlab001 --container-name output-container --output table --auth-mode login
az storage blob list --account-name sariskpocdevlab001 --container-name application2 --output table --auth-mode login
az storage blob delete --account-name sariskpocdevlab001 --container-name input-container --name input.json --output table --auth-mode login
