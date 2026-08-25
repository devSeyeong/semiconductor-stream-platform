#!/bin/bash
# EC2 최초 부팅 시 1회 실행되는 부트스트랩 (provision-ec2.sh 가 --user-data 로 넘긴다).
# Amazon Linux 2023 기준.
set -euxo pipefail

dnf update -y
dnf install -y docker git

systemctl enable --now docker
usermod -aG docker ec2-user

# docker compose v2 (플러그인)
mkdir -p /usr/local/lib/docker/cli-plugins
curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" \
    -o /usr/local/lib/docker/cli-plugins/docker-compose
chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

# 스왑 2GB — 8GB 에 JVM 3개(ZK/Kafka/Neo4j)가 올라가므로 안전판을 둔다.
# 상시로 쓰이면 느려지지만, 순간적인 피크에서 OOM Kill 을 막아준다.
if [ ! -f /swapfile ]; then
    dd if=/dev/zero of=/swapfile bs=1M count=2048
    chmod 600 /swapfile
    mkswap /swapfile
    swapon /swapfile
    echo '/swapfile none swap sw 0 0' >> /etc/fstab
fi

echo "bootstrap done" > /var/log/bootstrap-complete