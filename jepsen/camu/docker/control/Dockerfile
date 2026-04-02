FROM clojure:temurin-21-lein-bookworm

RUN apt-get update && apt-get install -y \
    openssh-client sshpass gnuplot curl iproute2 iputils-ping git \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL https://dl.min.io/client/mc/release/linux-amd64/mc -o /usr/local/bin/mc && \
    chmod +x /usr/local/bin/mc

# Generate SSH key for connecting to nodes
RUN ssh-keygen -t rsa -N "" -f /root/.ssh/id_rsa && \
    cp /root/.ssh/id_rsa.pub /root/.ssh/authorized_keys

WORKDIR /jepsen/camu

# Initialize a git repo so Jepsen's log_test_start! can read version info
RUN git init && git config user.email "jepsen@test" && git config user.name "jepsen"

# Pre-fetch dependencies
COPY project.clj /jepsen/camu/
RUN lein deps

COPY . /jepsen/camu/

CMD ["bash"]
