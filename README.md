# Integrity Checks for Join Queries
Prototype developed for [two master's theses](#authors--references) in Computer Science 
(Laurea Magistrale in Informatica, LM - 18) based on state-of-the-art
work on integrity checks for join queries in the cloud.

It implements a novel approach which bases the integrity checks on counting the number of 
occurrences for the join attribute values (__multiple occurrence approach__) 
in both a centralized and a distributed scenario. 

## Authors & References
- [Federica Vicini](https://github.com/FediV)
- [Michele Zenoni](https://github.com/Levyathanus)

```
@mastersthesis{vicini2023,
    author = {Vicini, Federica},
    title = {Integrità di join nel cloud: un approccio basato sul conteggio delle occorrenze},
    school = {Università degli Studi di Milano},
    year = {2023},
    month = {10}
}

@mastersthesis{zenoni2023,
    author = {Zenoni, Michele},
    title = {Integrità di join distribuiti in presenza di worker fidati e non fidati},
    school = {Università degli Studi di Milano},
    year = {2023},
    month = {10}
}
```

## Requirements
Tested on Windows 10 and Windows 11, requires:
- Java 17.0.6+
- a local installation of 
[Apache Derby v10.16.1.1](https://db.apache.org/derby/releases/release-10_16_1_1.cgi) in `C:\Apache`

Dependencies are installed through Gradle and they are listed in the `build.gradle` file.

## Project structure
```bash
integrity-checks-for-join-queries:.
│   .gitignore
│   build.gradle
│   derby.log
│   gradle.properties
│   gradlew
│   gradlew.bat
│   README.md
│   settings.gradle
│
├───config
│       client.config.json
│       db.config.json
│       disease.txt
│       name.txt
│       simulation.config.json
│           
├───GUI
│      	GUI.py
│   	state.json
│      	write_join_proportions.py
│                       
├───jars
│       ComputationalServer.jar
│       Main.jar
│       StorageServer.jar
│       
├───out
│       ComputationalServerOutput.txt
│       StorageServerOutputL.txt
│       StorageServerOutputR.txt
│       
├───src
│   └───main
│       └───java
│           │   Main.java
│           │   
│           ├───client
│           │   │   HttpServerThread.java
│           │   │   IntegrityViolationException.java
│           │   │   RestClient.java
│           │   │   TamperingException.java
│           │   │   
│           │   └───services
│           │           ConfigurationService.java
│           │           ResultService.java
│           │           
│           ├───communication
│           │       ClientConfigFile.java
│           │       DataConfigFile.java
│           │       DbConfigFile.java
│           │       DistributedJoinQueryMessage.java
│           │       JoinQueryMessage.java
│           │       QueryMessage.java
│           │       ResultMessage.java
│           │       TwinCondition.java
│           │       
│           ├───model
│           │       DataGenerator.java
│           │       QueryHandler.java
│           │       
│           ├───server
│           │   │   ComputationalServer.java
│           │   │   StorageServer.java
│           │   │   Worker.java
│           │   │   
│           │   └───services
│           │           ComputationService.java
│           │           DataGenerationService.java
│           │           QueryHandlingService.java
│           │           
│           ├───simulation
│           │       SimulationConfigFile.java
│           │       SimulationOutput.java
│           │       
│           └───utility
│                   CustomJsonParser.java
│                   Encryption.java
│                   Logger.java
│                   LogLevel.java
│                   ModelUtils.java
│                   Network.java
│                   PairDeserializer.java
│                   PairSerializer.java
│                   Statistics.java
│                   TextColor.java
│                   TripleDeserializer.java
│                   TripleSerializer.java
│                   
└───stats
        stats_occurrences.json
        stats_twins.json
```

## License
TBD