FROM arm64v8/ubuntu:22.04

# Install compiler
RUN apt-get update && apt-get install -y build-essential g++

# Copy code
COPY main.cpp /app/
WORKDIR /app

# Compile and run
RUN g++ -o my_program main.cpp
CMD ["./my_program"]