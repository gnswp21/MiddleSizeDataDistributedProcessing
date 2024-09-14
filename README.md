# MiddleSizeDataDistributedProcessing
실제 프로젝트에서 중간 규모(50 GB 내외)의 데이터를 처리 해야할 일이 종종 발생합니다.
하지만, 중간 규모의 데이터는 단일 노드에서 진행하기엔 크고, 대규모 클러스터에서 처리하기엔 너무 작습니다.
이런 상황에 필요한 것이 중간 규모의 클러스터입니다.

이 프로젝트에서는 세 종류의 EKS 클러스터를 구성해 중간 규모의 데이터를 EMR on Eks(Pyspark)를 통해 처리하는데 속도적, 비용적으로 적합한 클러스터 구성, 스파크 자원 분배율을 튜닝합니다.
또한, 이 튜닝 과정을 Airflow를 통해 관리합니다.

1. 중간 규모의 데이터셋의 효율적인 처리를 위한 튜닝
2. Eks 생성 -> EMR on Eks Job 실행 -> Prometheus를 통해 지표 측정 -> 지표를 바탕으로 튜닝
3. 간단하고 병렬적인 튜닝을 위한 Airflow를 통한 스케쥴링 (클러스터의 생성, Job 실행, 지표 측정, 클러스터의 삭제)

# 성과
1. 50기가 데이터에 적합한 구성 찾기
2. emr on eks로 해서 호스트 없이 사용 가능, 병렬적으로 실행가능, 간단한 설치와 삭제
3. airflow로 설치부터 실행삭제까지 관리하므로 end to end이면서 확장적인 설계


#  구조
![architecture](architecture.jpg).
## 데이터셋
https://www.kaggle.com/datasets/bwandowando/strong-passwords-in-rockyou2024-txt
History: https://en.wikipedia.org/wiki/RockYou
캐글에서 utf-8으로 인코딩된 암호를 데이터셋으로 사용합니다. 총 160기가의 데이터지만 이번 프로젝트에서는 50기가의 데이터만을 사용합니다.
## 데이터 흐름
1. ec2노드에서 파이썬 캐글 API 통해 캐글에서 데이터셋을 수집
2. boto3 aws api로 s3에 데이터셋 저장
3. EMR on EKS Pyspark로 s3에 저장된 데이터 읽어와 처리
4. 각각의 잡을 실행 후 사용된 시간, cpu, 메모리, 네트워크 IO를 프로메테우스 쿼리를 통해 지표측정 후 저장
5. 데이터 처리 결과를 s3 버켓에 저장

## 클러스터 튜닝
1. 세 개의 eks 클러스터를 생성합니다.
2. 각각의 eks 클러스터에서 emr 가상 클러스터를 생성합니다.
3. 각 Emr 가상 클러스터에 Emr on Eks 잡을 실행합니다. 각 클러스터에서 4번의 잡을 실행해 총 12개의 잡이 실행됩니다.
4. 각각의 잡 실행 후, 메트릭을 프로메테우스를 통해 수집하여 s3에 저장합니다.
5. 모든 잡이 종료된 클러스터부터 삭제합니다.
5. 1~5 과정을 Airflow dag를 통해 자동화합니다.
6. 수집된 지표를 통해 클러스터의 구성과 스파크 잡의 리소스 구성을 튜닝합니다.

## Job
잡은 잡에서 실행되는 테스크의 종류, 데이터의 양에 따라 4종류로 구분되어 있습니다.
- 테스크1: 간단한 테스크 - 텍스트의 첫글자에 따라 그룹 후 갯수를 카운트
- 테스크2: 비교적 복잡한 테스크 - 텍스트가 *강한 암호에 맞는지 검사 후, 강한 암호의 수를 세기
  (* 강한 암호 : 8 ~ 32 글자,1개 이상의 대문자,  1개 이상의 소문자,  1개 이상의 숫자,  1개 이상의 특수문자,  사전에 등재된 단어 아님)
- 5GB 데이터: 데이터의 일부
- 50GB 데이터: 전체 데이터

### Job 종류 
1. Job1: 테스크1, 5기가 데이터
2. Job2: 테스크2, 5기가 데이터
3. Job3: 테스크1, 50기가 데이터
4. Job4: 테스크2, 50기가 데이터



# 결과
m5.xlarge : 0.236 달러/h




## 깃 컨벤션
| Tag Name         | Description                                                                                   |
| ---------------- | --------------------------------------------------------------------------------------------- |
| Feat             | 새로운 기능을 추가                                                                            |
| Fix              | 버그 수정                                                                                     |
| Design           | CSS 등 사용자 UI 디자인 변경                                                                  |
| !BREAKING CHANGE | 커다란 API 변경의 경우                                                                        |
| !HOTFIX          | 급하게 치명적인 버그를 고쳐야하는 경우                                                        |
| Style            | 코드 포맷 변경, 세미 콜론 누락, 코드 수정이 없는 경우                                         |
| Refactor         | 프로덕션 코드 리팩토링                                                                        |
| Comment          | 필요한 주석 추가 및 변경                                                                      |
| Docs             | 문서 수정                                                                                     |
| Test             | 테스트 코드, 리펙토링 테스트 코드 추가, Production Code(실제로 사용하는 코드) 변경 없음       |
| Chore            | 빌드 업무 수정, 패키지 매니저 수정, 패키지 관리자 구성 등 업데이트, Production Code 변경 없음 |
| Rename           | 파일 혹은 폴더명을 수정하거나 옮기는 작업만인 경우                                            |
| Remove           | 파일을 삭제하는 작업만 수행한 경우                                                            |
