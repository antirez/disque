FROM ubuntu:15.04
MAINTAINER antirez antirez@gmail.com

# Install tools
RUN apt-get -y update
RUN apt-get install -y make
RUN apt-get install -y gcc

# Add disque
ADD . /disque
WORKDIR /disque

# Build project
RUN make

# Expose port
EXPOSE 7711

# Run server
CMD /disque/src/disque-server