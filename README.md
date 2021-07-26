# qe_dask_benchmark

 - The purpose of this repo is to have qe_dask_benchmark test cases saved before they are merged into the benchmark repo

  ## Environments
 - Workspace compute environment
  	 - Base Image URI : ```quay.io/domino/pre-release-environments:domino-minimal-environment.nitinm.2021-07-19``` 

  	 - Dockerfile Instructions
```
  	 	RUN pip install \
  	 		ray[all]==1.3.0 \   
  	 		pandas \   
  	 		pyyaml==5.4.1 \   
  	 		blosc==1.9.2 \
  	 		dask==2021.06.0 \
  	 		dask distributed==2021.06.0 \
  	 		lz4==3.1.1 \
  	 		msgpack==1.0.0 \
  	 		numpy==1.18.1 \
  	 		matplotlib==3.4.2 \
  	 		seaborn==0.11.1 \
  	 		scikit-learn==0.24.2 \
  	 		tables==3.6.1 \
  	 		dask_ml==1.9.0
```

 - Pluggable Workspace Tools:
```
		  jupyterlab:
		  title: "JupyterLab"
		  iconUrl: "/assets/images/workspace-logos/jupyterlab.svg"
		  start: [  /opt/domino/workspaces/jupyterlab/start ]
		  httpProxy:
		    internalPath: "/{{ownerUsername}}/{{projectName}}/{{sessionPathComponent}}/{{runId}}/{{#if pathToOpen}}tree/{{pathToOpen}}{{/if}}"
		    port: 8888
		    rewrite: false
		    requireSubdomain: false
```


 - Dask cluster compute environment
  	 	
   - Base Image URI : ```daskdev/dask:2021.6.2```

   - Dockerfile Instructions
  	 	```
  	 	   RUN pip install \
  	 	    blosc==1.9.2 \
		    dask==2021.06.0 \
		    dask distributed==2021.06.0 \
		    dask_ml==1.9.0 \
		    lz4==3.1.1 \
		    msgpack==1.0.0 \
		    numpy==1.18.1```


   - Dask workspace settings - DFS project
     - Workspace compute environment - select the environment with the above workspace compute environment
     - Workspace IDE : JupyterLab
     - Hardware Tier : Medium

     - Compute cluster - Attach compute cluster - Dask 
     - Number of Workers - 3
     - Worker Hardware Tier - Medium
     - Scheduler Hardware Tier - Medium
     - Cluster Compute Environment - select the environment with the above Dask cluster compute environment

     - Check the checkbox - Dedicated local storage per worker and enter 30GiB

 ## Steps to run the test/project:

  - Start the workspace with the above configuration
  - Start a terminal and run the below command
		cd /mnt  
		wget https://github.com/ddl-abishek/qe_dask_benchmark/archive/refs/heads/main.zip

  - unzip the downloaded dataset
		unzip qe_dask_benchmark.zip

  - Navigate to the dataset directory and download the dataset

  		   cd /domino/datasets/local/<domino-project-name>
  		   wget https://dsp-workflow.s3.us-west-2.amazonaws.com/nyc-parking-tickets.zip
  		   unzip nyc-parking-tickets.zip

  - Naviagate to 
  		```cd /mnt```

  - Edit the config file:
  		Replace every occurence of ```"/mnt/data/qe_dask_GBP"``` to ```"/mnt/<domino-project-name>"```

  - Run the preprocess script (This cleans the dataset)
  		```python preprocess.py```

  - Run the clustering script
  		```python k_means_clustering.py```
