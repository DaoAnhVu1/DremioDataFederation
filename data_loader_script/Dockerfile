# Use the official Node.js image as base
FROM node:latest

# Set working directory inside the container
WORKDIR /app

# Copy package.json and package-lock.json to the working directory
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy the data folder to the working directory
COPY data ./data

# Copy the script.js and script.postgres.js files to the working directory
COPY script.sql.js .
COPY script.postgres.js .

# Copy the wait-for-dremio.sh script to the working directory
COPY wait-for-dremio.sh /usr/local/bin/wait-for-dremio.sh

# Copy the start-scripts.sh script to the working directory
COPY start-scripts.sh /usr/local/bin/start-scripts.sh

# Ensure the scripts are executable
RUN chmod +x /usr/local/bin/wait-for-dremio.sh
RUN chmod +x /usr/local/bin/start-scripts.sh

# Command to run the start-scripts.sh, but use the wait-for-dremio.sh as the entrypoint
ENTRYPOINT ["/usr/local/bin/wait-for-dremio.sh", "/usr/local/bin/start-scripts.sh"]
