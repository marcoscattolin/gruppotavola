# gruppotavola
Install python env into azure VM

Update packages:
```
sudo apt update
```

Install pip
```
sudo apt install python3-pip
```

Install requirements
```
pip install -r requirements.txt
```

Set PYTHONPATH env variable
```
export PYTHONPATH=/home/gruppotavola/gruppotavola/src
```

Set crontab jobs
```
crontab /home/gruppotavola/gruppotavola/cronjobs.txt
```
