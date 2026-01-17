# base Docker image that we will build on
FROM python:3.13.11-slim
 # set up our image by installing prerequistites; pandas in this case
RUN pip install pandas pyarrow
# set up the working directory inside the container
WORKDIR /app
# copy the script to the container.1st name is source file,2nd is destination
COPY pipeline.py pipeline.py
# define what to do first when the container run
# in this example,we will just run the script
ENTRYPOINT ["python","pipeline.py"]
