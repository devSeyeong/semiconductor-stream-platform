#!/bin/bash
# EC2 운영 헬퍼. 평소엔 인스턴스를 정지해두고 필요할 때만 켜는 운용을 전제로 한다.
#
#   ./deploy/ec2ctl.sh deploy     코드 배포 + 스택 기동 (최초 1회 & 코드 갱신 시)
#   ./deploy/ec2ctl.sh start      인스턴스 시작 → 컨테이너 자동 복구 → URL 출력
#   ./deploy/ec2ctl.sh stop       인스턴스 정지 (컴퓨팅 과금 0, 디스크만 유지)
#   ./deploy/ec2ctl.sh status     상태 + 대시보드 URL
#   ./deploy/ec2ctl.sh logs [svc] 컨테이너 로그
#   ./deploy/ec2ctl.sh ssh        SSH 접속
#   ./deploy/ec2ctl.sh env        서버의 DB 비밀번호 확인
#   ./deploy/ec2ctl.sh update-my-ip   내 공인 IP 가 바뀌었을 때 보안그룹 갱신
#   ./deploy/ec2ctl.sh destroy    인스턴스 종료 (디스크 포함 삭제)
#
# 인스턴스를 정지/시작하면 공인 IP 가 바뀐다. 이 스크립트는 매번 조회하므로
# 신경 쓸 필요 없다 (고정 IP(EIP)를 붙이면 월 $3.6 이 추가로 든다).
set -euo pipefail

REGION="${REGION:-ap-northeast-2}"
NAME="${NAME:-semiconductor-stream}"
KEY_NAME="${KEY_NAME:-$NAME}"
KEY_FILE="${KEY_FILE:-$HOME/.ssh/$KEY_NAME.pem}"
REPO_URL="${REPO_URL:-https://github.com/devSeyeong/semiconductor-stream-platform.git}"
REMOTE_DIR="/home/ec2-user/semiconductor-stream-platform"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BRANCH="${BRANCH:-$(git -C "$SCRIPT_DIR" rev-parse --abbrev-ref HEAD 2>/dev/null || echo main)}"

# 어느 계정을 상대로 도는지 먼저 확정한다. 이름 태그로 인스턴스를 찾는
# 스크립트라, 계정을 착각하면 엉뚱한 인스턴스를 건드릴 수 있다.
# shellcheck source=deploy/aws-guard.sh
source "$SCRIPT_DIR/aws-guard.sh"

# --------------------------------------------------------------------- #

instance_id() {
    local id
    id="$(aws ec2 describe-instances --region "$REGION" \
        --filters "Name=tag:Name,Values=$NAME" \
                  "Name=instance-state-name,Values=pending,running,stopping,stopped" \
        --query 'Reservations[0].Instances[0].InstanceId' --output text)"
    if [ "$id" = "None" ] || [ -z "$id" ]; then
        echo "인스턴스($NAME)를 찾을 수 없습니다. 먼저 ./deploy/provision-ec2.sh 를 실행하세요." >&2
        exit 1
    fi
    echo "$id"
}

instance_state() {
    aws ec2 describe-instances --region "$REGION" --instance-ids "$(instance_id)" \
        --query 'Reservations[0].Instances[0].State.Name' --output text
}

public_ip() {
    aws ec2 describe-instances --region "$REGION" --instance-ids "$(instance_id)" \
        --query 'Reservations[0].Instances[0].PublicIpAddress' --output text
}

require_running() {
    local state
    state="$(instance_state)"
    if [ "$state" != "running" ]; then
        echo "인스턴스가 '$state' 상태입니다. 먼저 './deploy/ec2ctl.sh start' 를 실행하세요." >&2
        exit 1
    fi
}

remote() {
    ssh -i "$KEY_FILE" -o StrictHostKeyChecking=accept-new \
        "ec2-user@$(public_ip)" "$@"
}

# --------------------------------------------------------------------- #

cmd_deploy() {
    require_running
    local ip; ip="$(public_ip)"
    echo "==> $ip 에 배포 (브랜치: $BRANCH)"

    # 부트스트랩(도커 설치) 완료 대기
    echo "==> 부트스트랩 확인..."
    for _ in $(seq 1 40); do
        if remote 'test -f /var/log/bootstrap-complete' 2>/dev/null; then break; fi
        echo "    도커 설치 대기중... (30초)"
        sleep 30
    done

    # 코드 배포
    remote "
        set -e
        if [ -d '$REMOTE_DIR/.git' ]; then
            cd '$REMOTE_DIR'
            git fetch --all --prune
            git checkout '$BRANCH'
            git reset --hard 'origin/$BRANCH'
        else
            git clone --branch '$BRANCH' '$REPO_URL' '$REMOTE_DIR'
        fi
    "

    # 최초 1회: 강한 비밀번호로 .env 생성 (기본 admin/admin 을 쓰지 않는다)
    remote "
        set -e
        cd '$REMOTE_DIR'
        if [ ! -f .env ]; then
            {
                echo 'PG_DB=semiconductor'
                echo 'PG_USER=admin'
                echo \"PG_PASSWORD=\$(openssl rand -hex 16)\"
                echo \"NEO4J_PASSWORD=\$(openssl rand -hex 16)\"
                echo 'GRAPH_ENABLED=1'
            } > .env
            chmod 600 .env
            echo '[deploy] .env 생성 (랜덤 비밀번호)'
        fi
    "

    echo "==> 이미지 빌드 + 스택 기동 (첫 빌드는 3~5분)"
    remote "cd '$REMOTE_DIR' && docker compose --profile app up -d --build"

    echo
    echo "배포 완료 → http://$ip:8501"
}

cmd_start() {
    local id; id="$(instance_id)"
    local state; state="$(instance_state)"
    if [ "$state" = "running" ]; then
        echo "이미 실행중입니다."
    else
        echo "==> 시작: $id"
        aws ec2 start-instances --region "$REGION" --instance-ids "$id" >/dev/null
        aws ec2 wait instance-running --region "$REGION" --instance-ids "$id"
        echo "==> 컨테이너 복구 대기 (restart 정책으로 자동 기동, 약 60초)"
        sleep 60
    fi
    cmd_status
}

cmd_stop() {
    local id; id="$(instance_id)"
    echo "==> 정지: $id  (이후 컴퓨팅 과금 없음, EBS 30GB 월 \$2.7 만 유지)"
    aws ec2 stop-instances --region "$REGION" --instance-ids "$id" >/dev/null
    aws ec2 wait instance-stopped --region "$REGION" --instance-ids "$id"
    echo "정지 완료."
}

cmd_status() {
    local state; state="$(instance_state)"
    echo "인스턴스 : $(instance_id)  [$state]"
    if [ "$state" = "running" ]; then
        local ip; ip="$(public_ip)"
        echo "공인 IP  : $ip"
        echo "대시보드 : http://$ip:8501"
        echo
        remote "cd '$REMOTE_DIR' 2>/dev/null && docker compose --profile app ps" 2>/dev/null \
            || echo "(아직 배포 전입니다 — ./deploy/ec2ctl.sh deploy)"
    fi
}

cmd_logs() {
    require_running
    remote "cd '$REMOTE_DIR' && docker compose --profile app logs --tail 100 -f ${1:-}"
}

cmd_ssh() {
    require_running
    exec ssh -i "$KEY_FILE" -o StrictHostKeyChecking=accept-new "ec2-user@$(public_ip)"
}

cmd_env() {
    require_running
    remote "cat '$REMOTE_DIR/.env'"
}

cmd_update_my_ip() {
    local my_ip sg_id
    my_ip="$(curl -fsS https://checkip.amazonaws.com)"
    sg_id="$(aws ec2 describe-security-groups --region "$REGION" \
        --filters Name=group-name,Values="$NAME-sg" \
        --query 'SecurityGroups[0].GroupId' --output text)"
    echo "==> 보안그룹 $sg_id 를 $my_ip/32 로 갱신"

    # 기존 22/8501 규칙을 걷어내고 현재 IP 로 다시 연다
    for port in 22 8501; do
        local cidrs
        cidrs="$(aws ec2 describe-security-groups --region "$REGION" --group-ids "$sg_id" \
            --query "SecurityGroups[0].IpPermissions[?FromPort==\`$port\`].IpRanges[].CidrIp" \
            --output text)"
        for cidr in $cidrs; do
            [ "$cidr" = "$my_ip/32" ] && continue
            aws ec2 revoke-security-group-ingress --region "$REGION" --group-id "$sg_id" \
                --protocol tcp --port "$port" --cidr "$cidr" >/dev/null
            echo "    제거: $port <- $cidr"
        done
        aws ec2 authorize-security-group-ingress --region "$REGION" --group-id "$sg_id" \
            --protocol tcp --port "$port" --cidr "$my_ip/32" >/dev/null 2>&1 \
            && echo "    허용: $port <- $my_ip/32" || true
    done
}

cmd_destroy() {
    local id; id="$(instance_id)"
    echo "!! $id 를 완전히 종료합니다. EBS 볼륨(수집 데이터 포함)도 함께 삭제됩니다."
    read -r -p "계속하려면 'yes' 입력: " ans
    [ "$ans" = "yes" ] || { echo "취소."; exit 0; }
    aws ec2 terminate-instances --region "$REGION" --instance-ids "$id" >/dev/null
    echo "종료 요청 완료."
}

case "${1:-status}" in
    deploy)        cmd_deploy ;;
    start)         cmd_start ;;
    stop)          cmd_stop ;;
    status)        cmd_status ;;
    logs)          cmd_logs "${2:-}" ;;
    ssh)           cmd_ssh ;;
    env)           cmd_env ;;
    update-my-ip)  cmd_update_my_ip ;;
    destroy)       cmd_destroy ;;
    *)
        echo "사용법: $0 {deploy|start|stop|status|logs [svc]|ssh|env|update-my-ip|destroy}" >&2
        exit 1 ;;
esac
