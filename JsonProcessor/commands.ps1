cd "C:\Users\pocadmin\Desktop\project"
# SPN login 
az login --tenant 03e82858-fc14-4f12-b078-aac6d25c87da --service-principal --username de3eddc0-2cbc-4835-a5a1-c0d32bb01ebe --password ULW8Q~U2Xer~jM4DK-LgPpmugusTANu9Xvu.tbgO

pip install azure-identity azure-mgmt-storage azure-mgmt-batch azure-storage-queue
pip install azure-identity azure-mgmt-resource azure-mgmt-storage azure-mgmt-batch azure-storage-queue azure-mgmt-authorization

# Create virtual environment
python -m venv venv

# Activate
.\venv\Scripts\activate   # Windows


# Install dependencies
pip install -r requirements.txt

pip install azure-storage-blob==12.19.0
pip install azure-identity==1.16.1
pip install azure-batch==14.0.0
pip install azure-batch --upgrade

# Data processing (for generator script)
pip install python-dateutil==2.8.2

python3 --version
pip install requests

az extension add --name azure-batch-cli-extensions
az --version


cd "C:\Users\pocadmin\Desktop\Scripts\python"
python3 batch_pool.py
python3 submit_task.py --pool-id mypool 

az batch pool create --account-name bariskpocdevlab001 --json-file pool_config_debug.json

az acr create --resource-group rg-risk-poc-batch-devlab-001 --name crriskpocdevlab001 --sku Premium

dotnet publish -c Release -r linux-x64 --output ./publish --self-contained


Docket install

Invoke-WebRequest -Uri https://desktop.docker.com/win/stable/Docker%20Desktop%20Installer.exe -OutFile DockerDesktopInstaller.exe

Run the installer
Start-Process -FilePath .\DockerDesktopInstaller.exe -ArgumentList "install" -NoNewWindow -Wait

.\acr_push.ps1
.\acr_login.ps1
az acr login -n crriskpocdevlab001 --expose-token


docker images
docker image prune -f
docker rmi processor-image:latest 
docker rmi crriskpocdevlab001.azurecr.io/processor-image:v1.0.0
docker rmi crriskpocdevlab001.azurecr.io/processor-image:latest 
docker rmi crriskpocdevlab001.azurecr.io/batchtask:latest
docker rmi batchtask:latest

docker rmi storageaccount-image:latest



docker push myapp:latest
docker push crriskpocdevlab001.azurecr.io/batchtask:v1.0.0

az acr repository show-tags --name crriskpocdevlab001 --repository batchtask

.\acr_push.ps1 
.\acr_login.ps1

docker build -t crriskpocdevlab001.azurecr.io/processor-image:v1.0.0 --platform linux/amd64 .


docker push crriskpocdevlab001.azurecr.io/processor-image:latest

docker push crriskpocdevlab001.azurecr.io/docker-app:v1.0.0

docker build -t processor-image .
docker tag crriskpocdevlab001.azurecr.io/processor-image:v1.0.0 crriskpocdevlab001.azurecr.io/processor-image:latest

verify push
az acr repository show-tags --name crriskpocdevlab001 --repository processor-image
az acr repository delete --name crriskpocdevlab001 --repository myapp --yes

az acr repository list --name crriskpocdevlab001 --output table
az acr repository show-manifests --name crriskpocdevlab001 --repository processor-image --query "[].{Tag:tags[0], Size:size}" --output table
az acr manifest list-metadata -r crriskpocdevlab001 -n batch-json-processor --output table

docker run --rm -it crriskpocdevlab001.azurecr.io/docker-batch:latest


docker run --rm -it `
    -e STORAGE_ACCOUNT_URL="https://sariskpocdevlab001.blob.core.windows.net" `
    -e INPUT_CONTAINER_NAME="input-container" `
    -e INPUT_BLOB_NAME="input-blob" `
    -e OUTPUT_CONTAINER_NAME="output-container" `
    -e OUTPUT_BLOB_NAME="output-blob" `
    -e USER_MANAGED_IDENTITY_CLIENT_ID="dfdba649-7ce3-42c5-9c83-8fe43f811afd" `
    -e AZURE_CLIENT_ID="dfdba649-7ce3-42c5-9c83-8fe43f811afd" `
    crriskpocdevlab001.azurecr.io/docker-batch:latest

Image available at:
  - crriskpocdevlab001.azurecr.io/docker-batch:latest
  - crriskpocdevlab001.azurecr.io/docker-batch:v1.0.0

  .\batch_pool.ps1 -poolid mypool

  az batch pool show --pool-id mypool --query "{id: id, state: state, allocationState: allocationState}" --output table

  az batch node list --pool-id mypool --query "[].[id, state]"--output table

  az batch job list --query "[].id"

  az batch job delete --job-id json-processing-20251110-194241 --yes --account-endpoint bariskpocdevlab001.switzerlandnorth.batch.azure.com 

  az batch task list --job-id samplejob --query "[].id" -o table

  az batch task show --job-id json-processing-20251111-095332 --task-id task-0

  az batch task delete --job-id json-processing-20251110-194241 --task-id task-0 --yes

  az batch pool delete --account-endpoint bariskpocdevlab001.switzerlandnorth.batch.azure.com --pool-id mypool --yes


  wsl --install

  New app



  # In PowerShell 7+
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
python nodepool.py

python submit_task.py --pool-id mypool


az batch pool create --json-file newnodepool.json

az batch node list --pool-id json-processor-pool   --query "[].[id, state]"--output table

az batch job list --query "[].id"
az batch job create --account-endpoint bariskpocdevlab001.switzerlandnorth.batch.azure.com --pool-id mypool --id samplejob


az batch task list --job-id json-processing-20251111-095332 --query "[].id" -o table

az batch task create --job-id sampleJob --json-file newtask.json


pip install -r requirements.txt

pip install pandas==2.1.4
pip isntall python-dateutil==2.8.2
pip install requests==2.32.4
pip install azure-storage-blob==12.19.0
pip install azure-identity==1.16.1
pip install azure-batch==14.0.0

# In PowerShell 7+
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
python generate_data.py --count 100 --files 3 --output .\samples\
 
 # In PowerShell 7+
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
python upload_storage.py --container input-container --path .\samples\

az storage container list --account-name sariskpocdevlab001 --output table --auth-mode login
az storage blob list --account-name sariskpocdevlab001 --container-name output-container --output table --auth-mode login

cd "C:\Users\pocadmin\Desktop\Project_2\scripts"

# In PowerShell 7+
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
python create_batch_pool2.py

az batch pool delete --account-endpoint bariskpocdevlab001.switzerlandnorth.batch.azure.com --pool-id myPool --yes


az batch node list --pool-id mypool --query "[].[id, state]"--output table
az batch node list --pool-id mypool
az batch pool show --pool-id mypool --query "{id: id, state: state, allocationState: allocationState}" --output table
az batch pool show --pool-id mypool

az batch job list --query "[].id"
az batch job delete --job-id json-processing-20251113-092321 --yes --account-endpoint bariskpocdevlab001.switzerlandnorth.batch.azure.com


az batch task list --job-id json-processing-20251113-072302 --query "[].id" -o table
az batch task show --job-id json-processing-20251112-084730 --task-id task-0

az storage container list --account-name sariskpocdevlab001 --output table --auth-mode login
az storage blob list --account-name sariskpocdevlab001 --container-name output-container --output table --auth-mode login

az storage blob download --account-name sariskpocdevlab001 --container-name output-container --name input-file.txt --file input-file.txt --auth-mode login
cat input-file.txt

Check access
az login --identity --client-id 8e381705-966d-40d9-ad7d-7ff107917a06
az identity show --name id-risk-poc-batch-devlab-002 --resource-group rg-risk-poc-batch-devlab-001

az role assignment list --assignee "/subscriptions/8839ae3c-b717-45d7-9944-c2f388001643/resourceGroups/rg-risk-poc-batch-devlab-001/providers/Microsoft.ManagedIdentity/userAssignedIdentities/id-risk-poc-batch-devlab-001" --role "AcrPull" --scope "/subscriptions/8839ae3c-b717-45d7-9944-c2f388001643/resourceGroups/rg-risk-poc-batch-devlab-001/providers/Microsoft.ContainerRegistry/registries/crriskpocdevlab001"

  
  # In PowerShell 7+
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
python submit_task.py --pool-id mypool


# In PowerShell 7+
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
python download-results.py --output .\results\


# List all jobs
az batch job list --output table

# Watch specific job
az batch job show --job-id <JOB_ID>

# List tasks in job
az batch task list --job-id <JOB_ID> --output table

# View task output
az batch task file download --job-id <JOB_ID> --task-id task-0 --file-path stdout.txt

Daily Usage 
# 1. Generate or receive new data files
python scripts\generate-synthetic-data.py --count 1000 --files 10

# 2. Upload to storage
python scripts\upload-to-storage.py --path .\samples\

# 3. Submit batch job (or automate with Azure Function)
python scripts\submit-batch-job.py --pool-id json-processor-pool

# 4. Download results
python scripts\download-results.py --output .\results\


Monitor job status:
az batch job show --job-id json-processing-20251113-092321 --account-name bariskpocdevlab001 --account-endpoint https://bariskpocdevlab001.switzerlandnorth.batch.azure.com

List tasks:
az batch task list --job-id json-processing-20251113-092321 --account-name bariskpocdevlab001 --account-endpoint https://bariskpocdevlab001.switzerlandnorth.batch.azure.com

View task output:
az batch task file download --job-id json-processing-20251113-092321 --task-id task-0 --file-path stdout.txt --destination ./logs/stdout.txt --account-name bariskpocdevlab001 --account-endpoint https://bariskpocdevlab001.switzerlandnorth.batch.azure.com

Download results when complete:
  python download_results.py --output ./results/