# ADR-001: AWS 기반 클라우드 인프라 사용

**Status**: Accepted  
**Date**: 2025-07-01

---

## 1. Context (배경 및 문제 정의)
현재 프로젝트는 데이터 수집, 저장, 분석 및 시각화를 포함한 전 과정을 포함하고 있으며,
팀원들이 각자 다른 장소에서 협업하는 상황입니다.
또한 고정된 인프라가 없고, 환경 구성이 유연해야 하며, 보안 및 접근 제어 또한 중요합니다.
이러한 배경에서 클라우드 환경이 필요하며, 적절한 클라우드 서비스를 선택해야 하는 상황입니다.

## 2. Decision (내린 결정)
우리는 클라우드 기반 개발 환경으로 AWS(Amazon Web Services) 를 사용하기로 결정했습니다.

## 3. Alternatives Considered (고려했던 대안들)
1. **Google Cloud Platform (GCP)**
   - 장점: BigQuery 등 분석 서비스가 강력하며 학습 리소스도 풍부
   - 단점: 팀 내 사용 경험 부족, IAM 구성이나 네트워크 설정이 상대적으로 어렵게 느껴짐
2. **Microsoft Azure**
   - 장점: 기업용 통합 솔루션 강점, MS 계정 기반 통합 용이
   - 단점: 실제 프로젝트 경험 부족, 데이터 분석 위주의 기능이 제한적
3. **On-Premise (로컬 서버 또는 개인 VM 사용)**
   - 장점: 초기 비용 없음
   - 단점: 협업 어려움, 네트워크 및 보안 구성 부담, 확장성 부족

## 4. Rationale (결정의 이유)
- AWS는 국내외 시장 점유율이 높고, 실무에서 활용 사례가 풍부함
- Redshift, Lambda, S3, CloudWatch, SNS 등 다양한 서비스와의 연계가 용이
- 교육 및 학습 자료가 많아 팀원 역량 강화에 유리
- IAM, VPC, CloudWatch 등으로 보안과 모니터링 환경 구성이 상대적으로 쉬움
- 커뮤니티 자료 및 공식 문서가 풍부하여 문제 해결이 빠름

## 5. Consequences (영향 및 결과)
- AWS의 과금 체계에 대한 학습이 필요하며, 사용량 관리가 중요함
- AWS 리소스 사용에 따른 비용 발생 및 예산 관리 필요
- 향후 데이터 파이프라인 및 분석 인프라와의 연동이 수월해짐

## 6. Related Decisions (관련 ADR 문서)

## 7. References (참고 자료)
- [과기정통부, 2023년 부가통신사업 실태조사 결과 발표](https://www.msit.go.kr/bbs/view.do?sCode=&mId=113&mPid=238&pageIndex=6&bbsSeqNo=94&nttSeqNo=3184748&searchOpt=ALL&searchTxt=)
- [AWS, Azure, Google Cloud 비교: 클라우드 서버 선택 가이드](https://germmen.tistory.com/entry/AWS-Azure-Go)
