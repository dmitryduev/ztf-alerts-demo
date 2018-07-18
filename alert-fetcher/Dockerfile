FROM python:3.6

# Install vim, git
RUN apt-get update && apt-get -y install apt-file && apt-file update && apt-get -y install vim && \
    apt-get install -y git

# place to keep our app, and alerts:
RUN mkdir -p /app
RUN mkdir -p /alerts

# install python libs
COPY code/requirements.txt /app/
RUN pip install Cython && pip install numpy
RUN pip install -r /app/requirements.txt

# copy over the code
ADD code/ /app/

# change working directory to /app
WORKDIR /app

# run bash / type commands manually
#CMD /bin/bash
# fetch alert demo set and ingest it into db
CMD python fetcher.py config.json 20180713 --demo