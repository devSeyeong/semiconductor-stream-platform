#!/bin/bash
# EC2 인스턴스 1대를 생성한다 (t3.large, 코어 구성용).
#
#   ./deploy/provision-ec2.sh
#
# 만드는 것: 키페어(없으면) → 보안그룹 → 인스턴스.
# 22(SSH)/8501(대시보드) 을 "실행한 PC 의 현재 공인 IP" 에서만 연다.
# 나머지 포트(Kafka/PG/Neo4j)는 compose 에서 루프백 바인딩이라 애초에 안 열린다.
#
# 주의: 이 스크립트는 과금되는 리소스를 만든다.
#
# 실행 전 EXPECT_ACCOUNT(+보통 AWS_PROFILE)을 지정해야 한다 — aws-guard.sh 참고.
set -euo pipefail

REGION="${REGION:-ap-northeast-2}"
NAME="${NAME:-semiconductor-stream}"
INSTANCE_TYPE="${INSTANCE_TYPE:-t3.large}"
VOLUME_SIZE="${VOLUME_SIZE:-30}"
KEY_NAME="${KEY_NAME:-$NAME}"
KEY_FILE="${KEY_FILE:-$HOME/.ssh/$KEY_NAME.pem}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# 어느 계정에 만드는지 먼저 확정한다 (틀리면 여기서 멈춘다)
# shellcheck source=deploy/aws-guard.sh
source "$SCRIPT_DIR/aws-guard.sh"

echo "==> 리전 $REGION / 타입 $INSTANCE_TYPE / 디스크 ${VOLUME_SIZE}GB"

# --- 현재 공인 IP ------------------------------------------------------- #
MY_IP="$(curl -fsS https://checkip.amazonaws.com)"
echo "==> 접근 허용 IP: $MY_IP (변경되면 update-my-ip 로 갱신)"

# --- 키페어 ------------------------------------------------------------- #
if aws ec2 describe-key-pairs --region "$REGION" --key-names "$KEY_NAME" >/dev/null 2>&1; then
    echo "==> 키페어 $KEY_NAME 이미 존재 (재사용)"
    if [ ! -f "$KEY_FILE" ]; then
        echo "!! 로컬에 $KEY_FILE 이 없습니다. 이 키로는 SSH 할 수 없습니다." >&2
        echo "   KEY_NAME 을 새 이름으로 바꿔 다시 실행하세요." >&2
        exit 1
    fi
else
    echo "==> 키페어 $KEY_NAME 생성 → $KEY_FILE"
    mkdir -p "$(dirname "$KEY_FILE")"
    aws ec2 create-key-pair --region "$REGION" --key-name "$KEY_NAME" \
        --query 'KeyMaterial' --output text > "$KEY_FILE"
    chmod 400 "$KEY_FILE"
fi

# --- 기본 VPC ----------------------------------------------------------- #
VPC_ID="$(aws ec2 describe-vpcs --region "$REGION" \
    --filters Name=isDefault,Values=true \
    --query 'Vpcs[0].VpcId' --output text)"
if [ "$VPC_ID" = "None" ]; then
    echo "!! 기본 VPC 가 없습니다. VPC 를 지정하도록 스크립트를 수정하세요." >&2
    exit 1
fi
echo "==> VPC: $VPC_ID"

# --- 보안그룹 ----------------------------------------------------------- #
SG_ID="$(aws ec2 describe-security-groups --region "$REGION" \
    --filters Name=group-name,Values="$NAME-sg" Name=vpc-id,Values="$VPC_ID" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || echo "None")"

if [ "$SG_ID" = "None" ]; then
    echo "==> 보안그룹 $NAME-sg 생성"
    SG_ID="$(aws ec2 create-security-group --region "$REGION" \
        --group-name "$NAME-sg" --vpc-id "$VPC_ID" \
        --description "semiconductor stream platform - ssh + dashboard" \
        --query 'GroupId' --output text)"
fi
echo "==> 보안그룹: $SG_ID"

for PORT in 22 8501; do
    aws ec2 authorize-security-group-ingress --region "$REGION" \
        --group-id "$SG_ID" --protocol tcp --port "$PORT" --cidr "$MY_IP/32" \
        >/dev/null 2>&1 && echo "    허용: $PORT <- $MY_IP/32" \
        || echo "    이미 허용됨: $PORT <- $MY_IP/32"
done

# --- AMI (Amazon Linux 2023, x86_64) ------------------------------------ #
AMI_ID="$(aws ssm get-parameters --region "$REGION" \
    --names /aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64 \
    --query 'Parameters[0].Value' --output text)"
if [ -z "$AMI_ID" ] || [ "$AMI_ID" = "None" ]; then
    echo "!! AL2023 AMI 를 찾지 못했습니다." >&2
    exit 1
fi
echo "==> AMI: $AMI_ID"

# --- 최종 확인 ---------------------------------------------------------- #
# 여기서부터 과금이 시작된다. 계정/리전/타입을 눈으로 한 번 더 보고 넘어간다.
# 자동화(CI 등)에서는 CONFIRM=yes 로 건너뛴다.
if [ "${CONFIRM:-}" != "yes" ]; then
    cat <<EOF

------------------------------------------------------------------------
 아래 설정으로 인스턴스를 생성합니다 (과금 시작)
------------------------------------------------------------------------
 계정   : $AWS_ACCOUNT
 리전   : $REGION
 타입   : $INSTANCE_TYPE  (+ gp3 ${VOLUME_SIZE}GB)
 키파일 : $KEY_FILE
------------------------------------------------------------------------
EOF
    read -r -p "계속하려면 'yes' 입력: " ans
    [ "$ans" = "yes" ] || { echo "취소."; exit 0; }
fi

# --- 인스턴스 ----------------------------------------------------------- #
INSTANCE_ID="$(aws ec2 run-instances --region "$REGION" \
    --image-id "$AMI_ID" \
    --instance-type "$INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$SG_ID" \
    --block-device-mappings "DeviceName=/dev/xvda,Ebs={VolumeSize=$VOLUME_SIZE,VolumeType=gp3,DeleteOnTermination=true}" \
    --user-data "file://$SCRIPT_DIR/user-data.sh" \
    --tag-specifications "ResourceType=instance,Tags=[{Key=Name,Value=$NAME}]" \
    --query 'Instances[0].InstanceId' --output text)"

echo "==> 인스턴스 생성됨: $INSTANCE_ID — 기동 대기중..."
aws ec2 wait instance-running --region "$REGION" --instance-ids "$INSTANCE_ID"

PUBLIC_IP="$(aws ec2 describe-instances --region "$REGION" \
    --instance-ids "$INSTANCE_ID" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)"

cat <<EOF

========================================================================
 생성 완료
========================================================================
 인스턴스 : $INSTANCE_ID
 공인 IP  : $PUBLIC_IP
 SSH      : ssh -i $KEY_FILE ec2-user@$PUBLIC_IP

 부트스트랩(도커 설치)에 2~3분 걸립니다. 완료 확인:
   ssh -i $KEY_FILE ec2-user@$PUBLIC_IP 'cat /var/log/bootstrap-complete'

 다음 단계: ./deploy/ec2ctl.sh deploy
========================================================================
EOF