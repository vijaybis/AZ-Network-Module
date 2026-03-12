 cd "C:\Users\pocadmin\Desktop\Project_2\scripts"
 az login --tenant 03e82858-fc14-4f12-b078-aac6d25c87da --service-principal --username de3eddc0-2cbc-4835-a5a1-c0d32bb01ebe --password ULW8Q~U2Xer~jM4DK-LgPpmugusTANu9Xvu.tbgO

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
python generate_data.py --count 100 --files 5 --output .\samples\
 
 # In PowerShell 7+
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
python upload_storage.py --container input-container --path .\samples\

# In PowerShell 7+
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
python create_batch_pool.py

 
  # In PowerShell 7+
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
python submit_task.py --pool-id mypool

az batch node list --pool-id mypool
az storage blob list --account-name sariskpocdevlab001 --container-name output-container --output table --auth-mode login


# In PowerShell 7+
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
python download_results.py --output .\results\


# In PowerShell 7+
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
python delete_pool.py


# In PowerShell 7+
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$env:PYTHONUTF8 = "1"
$env:PYTHONIOENCODING = "utf-8"
python complete_task.py

