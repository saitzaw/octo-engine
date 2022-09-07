## Test Environment 
- CPU: core i5 11 Gen @ 2.4 GHz
- RAM: 16 GB
- Ubuntu 20.04 LTS

## Installation 
- install openJDK 11
```shell
sudo apt-get update && sudo apt-get upgrade
sudo apt-get install openjdk-11-jdk -y
sudo apt-get install openjdk-8-jdk -y
```

- Java version switch
```shell
sudo update-alternatives --config java
```
Note: sometime, we need to install openJDK 8 too

- sbt Installation
```shell
sudo apt-get update
sudo apt-get install apt-transport-https curl gnupg -yqq
echo "deb https://repo.scala-sbt.org/scalasbt/debian all main" | sudo tee /etc/apt/sources.list.d/sbt.list
echo "deb https://repo.scala-sbt.org/scalasbt/debian /" | sudo tee /etc/apt/sources.list.d/sbt_old.list
curl -sL "https://keyserver.ubuntu.com/pks/lookup?op=get&search=0x2EE0EA64E40A89B84B2DF73499E82A75642AC823" | sudo -H gpg --no-default-keyring --keyring gnupg-ring:/etc/apt/trusted.gpg.d/scalasbt-release.gpg --import
sudo chmod 644 /etc/apt/trusted.gpg.d/scalasbt-release.gpg
sudo apt-get update
sudo apt-get install sbt
```

- Spark 3.3.0
```
wget https://dlcdn.apache.org/spark/spark-3.3.0/spark-3.3.0-bin-hadoop3.tgz
sudo tar -zxvf spark-3.3.0-bin-hadoop3.tgz
sudo mv spark-3.3.0-bin-hadoop3 /opt/spark3
```

- Intellij
```shell
wget https://download.jetbrains.com/idea/ideaIU-2022.2.1.tar.gz
sudo tar -zxvf ideaIU-2022.2.1.tar.gz
sudo mv ideaIU-2022.2.1 /opt/intellij
```

- Intellij Desktop Icon
```shell
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
Intellij > File > project Structure > project Setting[Modules] > select [+] > choose [Jars and Directors] 
Find the spark install path [suggested to install in /opt path]
```
/opt/spark/jars
```

## Export Spark home
# Run in command line
```shell
export SPARK_HOME=/opt/spark
```

## resources link
- JSON
https://github.com/spark-examples/spark-scala-examples/blob/master/src/main/resources/zipcodes.json
- CSV
https://github.com/spark-examples/spark-scala-examples/blob/master/src/main/resources/zipcodes.csv
- 150000 and 2M records test in Scala (download as 7z and change to csv > parquet)
https://eforexcel.com/wp/downloads-18-sample-csv-files-data-sets-for-testing-sales/
- Code example
https://sparkbyexamples.com/ 
