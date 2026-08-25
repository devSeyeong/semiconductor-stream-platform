#!/bin/bash
# 자격증명 가드 — provision-ec2.sh / ec2ctl.sh 가 맨 앞에서 source 한다.
#
# 왜 필요한가: aws CLI 는 아무 인자 없이 실행하면 그 PC 에 "현재 로그인돼
# 있는" 기본 자격증명을 조용히 집어 쓴다. 개인 계정과 배포용 계정을 함께
# 쓰는 환경에서는 엉뚱한 계정에 과금 리소스를 만들고 나서야 알게 된다.
#
# 그래서 배포 대상 계정 ID 를 사람이 직접 적게 만든다. EXPECT_ACCOUNT 가
# 비어 있거나 실제 호출자 계정과 다르면 아무 것도 하지 않고 멈춘다.
#
#   export AWS_PROFILE=deploy-portfolio      # 쓰려는 자격증명 선택
#   export EXPECT_ACCOUNT=123456789012       # 그 자격증명이 속한 계정 ID
#   ./deploy/provision-ec2.sh
#
# 액세스 키를 환경변수로 직접 넣어 쓰는 방식도 그대로 동작한다:
#   AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... EXPECT_ACCOUNT=... ./deploy/...

if [ -z "${EXPECT_ACCOUNT:-}" ]; then
    cat >&2 <<'EOF'
!! EXPECT_ACCOUNT 가 지정되지 않아 중단합니다.

   기본 자격증명이 실수로 쓰이는 것을 막기 위한 안전장치입니다.
   배포하려는 AWS 계정 ID 를 명시하세요:

     export AWS_PROFILE=<사용할 프로파일>      # 또는 AWS_ACCESS_KEY_ID/SECRET
     export EXPECT_ACCOUNT=<12자리 계정 ID>
     ./deploy/<스크립트>

   현재 자격증명이 어느 계정인지 확인만 하려면:
     aws sts get-caller-identity
EOF
    exit 1
fi

_ident="$(aws sts get-caller-identity --query '[Account,Arn]' --output text 2>&1)" || {
    echo "!! 자격증명 확인 실패 (aws sts get-caller-identity):" >&2
    echo "$_ident" >&2
    echo "   AWS_PROFILE=${AWS_PROFILE:-<미지정>} 이 유효한지 확인하세요." >&2
    exit 1
}

AWS_ACCOUNT="$(echo "$_ident" | awk '{print $1}')"
AWS_CALLER_ARN="$(echo "$_ident" | awk '{print $2}')"

if [ "$AWS_ACCOUNT" != "$EXPECT_ACCOUNT" ]; then
    cat >&2 <<EOF
!! 계정이 일치하지 않아 중단합니다.

   기대한 계정 : $EXPECT_ACCOUNT
   실제 계정   : $AWS_ACCOUNT
   호출자      : $AWS_CALLER_ARN
   프로파일    : ${AWS_PROFILE:-<미지정 — 기본 자격증명>}

   의도한 자격증명이 맞는지 확인한 뒤 다시 실행하세요.
EOF
    exit 1
fi

echo "==> 자격증명 확인: 계정 $AWS_ACCOUNT / ${AWS_CALLER_ARN} (profile=${AWS_PROFILE:-기본})"
