# Currency Exchange Data Pipeline End-To-End Python Data Engineering Project
Implement Complete Data Pipeline Data Engineering Project using RAPID Api 
* Integrating with RapidAPI and extracting Data [https://rapidapi.com/fyhao/api/currency-exchange/]
* Deploying code on AWS Lambda for Data Extraction
* Adding trigger to run the extraction automatically 
* Writing transformation function
* Building automated trigger on transformation function 
* Store transformed files on S3
* Using Snowpipe auto ingest the transformed data to SNowflake

## Steps for Creating AWS Lambda Layers using AWS Cloud9

* Create a Cloud 9 Environment
* Install the right Python version
	* wget https://www.python.org/ftp/python/3.9.10/Python-3.9.10.tgz
	* tar xvf Python-3.9.10.tgz
	* cd Python-*/
	* ./configure --enable-optimizations
	* sudo make altinstall
	* cd ..
* Package your libraries
	* mkdir packaging
	* cd packaging
	* python3.9 -m venv layer_package
	* source layer_package/bin/activate
	* pip install requests spotipy
	* deactivate
	* mkdir python
	* cp -r layer_package/lib/python3.9/site-packages/* python/
	* zip -r lambda_layer.zip python
* Create your new Lambda Layer
	* aws lambda publish-layer-version --layer-name my_lambda_layer --zip-file fileb://lambda_layer.zip --compatible-runtimes python3.9
* Teardown Cloud9 environment

