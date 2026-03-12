 cd "C:\Users\pocadmin\Desktop\Project_2\scripts"
 az login --tenant 03e82858-fc14-4f12-b078-aac6d25c87da --service-principal --username de3eddc0-2cbc-4835-a5a1-c0d32bb01ebe --password ULW8Q~U2Xer~jM4DK-LgPpmugusTANu9Xvu.tbgO


### Pool ###
az batch pool create --json-file create_batch_pool2.json

az batch pool show --pool-id mypool --query "{id: id, state: state, allocationState: allocationState}" --output table

az batch pool delete --account-endpoint bariskpocdevlab001.switzerlandnorth.batch.azure.com --pool-id mypool --yes

az batch node list --pool-id mypool

az batch node list --pool-id mypool --query "[].[id, state]"--output table



### Job ###

az batch job list --query "[].id"

az batch job create --account-endpoint bariskpocdevlab001.switzerlandnorth.batch.azure.com --pool-id mypool --id samplejob

az batch job delete --job-id json-processing-20251114-103935 --yes --account-endpoint bariskpocdevlab001.switzerlandnorth.batch.azure.com 



### Task ###

az batch task list --job-id json-processing-20251111-095332 --query "[].id" -o table

az batch task create --job-id sampleJob --json-file newtask.json

az batch task list --job-id samplejob --query "[].id" -o table

az batch task show --job-id json-processing-20251114-101118 --task-id task-0

az batch task delete --job-id json-processing-20251110-194241 --task-id task-0 --yes


Managed ID dentity 
az batch pool set --account-name bariskpocdevlab001 --account-key <BatchAccountKey> \
  --account-endpoint <BatchAccountEndpoint> \
  --pool-id <PoolID> \
  --identity-type UserAssigned \
  --identity-scope <UserManagedIdentityResourceId>