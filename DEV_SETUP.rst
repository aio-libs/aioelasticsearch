=====================================
Getting started with aioelasticsearch
=====================================

| There you will find full description to get aioelasticsearch library up and ready.
| For this example Ubuntu 18.04/macOS X and Python 3.6 were used.

The main steps:
===============

#. Clone this github repo.
#. Install and configure docker for this project:

   **On Linux:**
    
   .. code:: bash

       sudo apt install docker.io
       systemctl start docker
       systemctl enable docker

   After that, you need to add your current user to the 'docker' group:

   .. code:: bash

       usermod -aG docker $USER
       
   **On macOS X:**
   
   Follow instructions described `here <https://docs.docker.com/docker-for-mac/install/>`__.
   
#. You might also want to check the docker version:

   .. code:: bash

       docker --version

#. To verify that docker is configured correctly, run following command:

   .. code:: bash

       docker ps -a

   If it doesn't raise any errors, you can proceed with next steps.

#. Create and activate virtual env in downloaded folder:

   if you use virtualenv:

   .. code:: bash

       virtualenv -p python3 <envname>
       source <envname>/bin/activate

   if you use Anaconda:

   .. code:: bash

       conda create -n <envname>
       conda activate <envname>

   Also you can use another method, like
   `venv <https://docs.python.org/3/library/venv.html>`__.

#. Install required packages from requirements.txt:

   .. code:: bash

       pip install -r requirements.txt
       
#. To be sure that aioelasticsearch is in development mode run following command:

   .. code:: bash

       pip install -e .

#.  Great! Now you can run some tests, to see if everything works correctly:

    **On Linux:**

    .. code:: bash

      pytest tests

    **On macOS X:**

    .. code:: bash

        pytest --local-docker

    | Note: the first time you will have to wait while docker downloads the ElasticSearch image (~600 MB).
    | This may take some time (usually up to 5 minutes).
    | All further test runs will take less time (usually up to 3 minutes).

