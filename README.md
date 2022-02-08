# qe_dask_benchmark

 - The purpose of this repo is to have qe_dask_benchmark test cases saved before they are merged into the benchmark repo

  ## Environments
 - Workspace compute environment
  	 - Base Image URI : ```quay.io/domino/pre-release-environments:domino-minimal-environment.nitinm.2021-07-19``` 
(THE DOCKERFILE INSTRUCTIONS ARE BASED ON DOMINO 4.6. PLEASE REFER TO https://dominodatalab.atlassian.net/wiki/spaces/ENG/pages/1812824298/Fleetcommand+Deployment+specifications+for+running+DSP+Workflow+tests FOR UP TO DATE DOCKERFILE INSTRUCTIONS)
  	 - Dockerfile Instructions
```
RUN pip install "dask[complete]"==2021.6.2
RUN pip install dask-ml

RUN pip install \
    ray[all]==1.3.0 \   
    pandas \   
    torch==1.8.0 \   
    torchvision==0.9.0 \   
    librosa==0.8.0 \   
    pyyaml==5.4.1 \   
    tensorboard==2.4.1 \
    matplotlib==3.4.2 \
    seaborn==0.11.1 \
	blosc==1.9.2 \
	lz4==3.1.1 \
	msgpack==1.0.0 \
	numpy==1.18.1

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
	lz4==3.1.1 \
	msgpack==1.0.0 \
	numpy==1.18.1 \
	matplotlib==3.4.2 \
	seaborn==0.11.1 \
	scikit-learn==0.24.2 \
	tables==3.6.1 \
	dask_ml==1.9.0
```


   - Dask workspace settings - DFS project
     - Workspace compute environment - select the environment with the above workspace compute environment
     - Workspace IDE : JupyterLab
     - Hardware Tier : Large

     - Compute cluster - Attach compute cluster - Dask 
     - Number of Workers - 12
     - Worker Hardware Tier - Large
     - Scheduler Hardware Tier - Large
     - Cluster Compute Environment - select the environment with the above Dask cluster compute environment

     - Check the checkbox - Dedicated local storage per worker and enter 30GiB

 ## Steps to run the test/project:

  - Create a DFS project with the name "qe_dask_benchmark_GBP". Start the workspace with the above configuration
  - Start a terminal and run the below command
		cd /mnt  
		wget https://github.com/ddl-abishek/qe_dask_benchmark/archive/refs/heads/main.zip

  - unzip the downloaded zip file
		unzip qe_dask_benchmark.zip

  - Navigate to the dataset directory and download the dataset

  		   cd /domino/datasets/local/<domino-project-name>
  		   wget https://dsp-workflow.s3.us-west-2.amazonaws.com/nyc-parking-tickets.zip
  		   unzip nyc-parking-tickets.zip

  - Naviagate to 
  		```cd /mnt```

  - Edit the ```config.yml``` file:
  		Replace every occurence of ```"/mnt/data/qe_dask_benchmark_GBP"``` to ```"/domino/datasets/local/qe_dask_benchmark_GBP"```
  		Replace  ```"/mnt/artifacts/results"``` with ```"/mnt/results"```      

  - Run the preprocess script (This cleans the dataset)
  		```python preprocess.py```

  - Run the clustering script
  		```python k_means_clustering.py```
		
  - Finally to get the scatter plots 
  		```python scatter_plot.py```

  - After each one of these scripts complete, you can see the performance report in 
  		```/mnt/artifacts/results```
