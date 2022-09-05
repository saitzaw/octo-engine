## Test environment 
- CPU: core i5 11 Gen @ 2.4 GHz
- RAM: 16 GB
- Ubuntu 20.04 LTS

## Installation 
- install openJDK 11
```
sudo apt-get update && sudo apt-get upgrade
sudo apt-get install openjdk-11-jdk -y
```
- Java version switch
```
sudo update-alternatives --config java
```
Note: sometime, we need to install openJDK 8 too

- Spark 3.3.0
```
wget https://dlcdn.apache.org/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz
sudo tar -zxvf spark-3.3.0-bin-hadoop3.tgz
sudo mv spark-3.3.0-bin-hadoop3 /opt/spark3
```

- Intellij
```
wget https://download.jetbrains.com/idea/ideaIU-2022.2.1.tar.gz
sudo tar -zxvf ideaIU-2022.2.1.tar.gz
sudo mv ideaIU-2022.2.1 /opt/intellij
```

- Intellij Desktop Icon
```
[Desktop Entry]
Version=13.0
Type=Application
Terminal=false
Icon[en_US]=/opt/intellij/bin/idea.png
Name[en_US]=IntelliJ
Exec=/opt/intellij/bin/idea.sh
Name=IntelliJ
Icon=/opt/intellij/bin/idea.png
```

## Java File Path
Intellij
```
``
