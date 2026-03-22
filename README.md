# Overview
Collection of 3 APIs to manage a messaging system architecture. 
Developed in Python and using Neo4j, Redis, and MongoDB for data management.

## Dependencies
An export of the Conda environment used for this project is provided in the file `environment.yml`. To recreate it:

conda create --name <env_name> -f environment.yml

## Execution Guide
All the code is located in the `src` directory, and a full usage example can be found in `src/main.ipynb`.

To run the project, you may need to modify the client factory functions for each database system, in case your services are running on non-default ports or you want to use different databases.

Additionally, you might need to update the Neo4j password, as it requires authentication (it is the second value in the tuple within the notebook).

In `src/demo_subsystems.ipynb`, the API of each individual subsystem is tested. These include CRUD operations and additional utilities for specific queries required in the App.

The business logic of the application is implemented in `src/services.py`.
