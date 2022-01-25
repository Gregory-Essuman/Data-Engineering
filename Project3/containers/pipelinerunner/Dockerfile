FROM python:3.9.5

# set up location of code
WORKDIR /code
ENV PYTHONPATH=/code/src

# install cron
RUN apt-get update && apt-get install cron -y

# install python requirements
ADD ./containers/pipelinerunner/requirements.txt requirements.txt
RUN pip install -r requirements.txt

# copy repo
COPY ./ /code/

# ref: https://stackoverflow.com/questions/37458287/how-to-run-a-cron-job-inside-a-docker-container
# Copy pull_bitcoin_exchange_info file to the cron.d directory
COPY /scheduler/pull_covidM_monitor_info /etc/cron.d/pull_covidM_monitor_info
 
# Give execution rights to wait-for. This command utility(wait-for) is used in case a service depends on another and that service isn't running yet.
#RUN chmod +x ./wait-for.sh 

# Give execution rights to dc entrypoint script to run as main cmd while waiting for warehouse
#RUN chmod +x ./dc-entrypoint.sh 

# Give execution rights on the cron job
RUN chmod 0644 /etc/cron.d/pull_covidM_monitor_info

# Apply cron job to crontab file.
RUN crontab /etc/cron.d/pull_covidM_monitor_info

# Create the log file to be able to run tail
RUN touch /var/log/cron.log
 
# Entry point. Used in case wait-for is applied.
#ENTRYPOINT ./dc-entrypoint.sh 

# Run cron
CMD cron && tail -f /var/log/cron.log
