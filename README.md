
<h1 align="center">PerpV2 REST API</h1>

## Theme
TLDR; a classic api framed over the subgraph  
This repo contains the necessary code to setup a mongodb caching mechanism for Perpetual protocols's subgraph. 

## Caveat
Please note that this repository is just an initil release, so bugs are likely to be found when dealing with its methods. As we move on we would try to improve the underlying mechanisms of data retrieval and calculation.  
Right now almost all data are retrieved with a maximum of 10 mitues delay so please take this into account when using the api.

## Setup
- Get a mongodb instance up
- Setup the crontab to call files within the `db/` directory  
- Setup NgineX and a uwsgi instance to serve the `server.py`
- Note than you need to manually update the `assets.json` file once new market are added  
