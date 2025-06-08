# ✨ **리그 오브 레전드 데이터 분석 프로젝트**

### 실행

> dockerfile을 이용한 dev-container 환경을 제공합니다. 하지만, metabase 컨테이너 빌드/실행을 위해선 로컬에서 진행해주세요.

1. docker build and run metabase image for create dashboard
  ```sh
  docker build metabase/. --tag metaduck:latest
  docker run --name metaduck -d -p 3000:3000 -m 2GB -e MB_PLUGINS_DIR=/home/plugins -v {your_base_dir}/data:/home/data metaduck
  ```
2. set RIOT API key
  ```sh
  # .env
  RIOT_KEY={your_api_key}
  ```
3. install dependencies
  ```sh
  poetry install
  ```
4. run pipeline
  ```sh
  poetry run python -m scripts.run_pipeline --pipeline_path pipelines/default/pipeline.py --config_path pipelines/default/config.yaml
  ```


### Metabase로 구성한 Dashboard 화면
![image](https://github.com/user-attachments/assets/254ea8ce-066e-49dc-8d24-f7e41dbe6a70)

### 프로젝트 후기
- 사용자는 요구 명세서(.yaml)를 작성하여 Request하기만 하면 Response 단계를 신경쓸 필요 없이 원하는 결과를 받을 수 있도록 설계하였습니다. 단, 요청이나 답변의 형태가 달라지거나 필요한 정보가 누락될 경우 문제가 발생하므로 pydantic 문법을 활용하여 주고받는 데이터에 대한 제약을 강화했습니다. 유지보수 측면도 고려하여 폴더 구조를 먼저 Component / Module / Pipeline 으로 나누어 개발하였는데 원하는 설계대로 만들어진 것 같아 만족합니다.
- git commit convention은 지킨다고 지켰으나 작은 수정 사항들이 모여 커밋 로그가 보기 어려워졌습니다. 대부분의 그러한 커밋들이 놓친 에러를 수정하는 과정에서 생겼는데, 다음 프로젝트부턴 test 코드를 확실하게 작성하고 커밋 전에 기존 코드와의 충돌이 없는지, 문제가 발생하지는 않는지 확인하는 과정을 반드시 추가하고자 합니다.
- 처음 계획의 RabbitMQ를 활용한 MessageQueue 구현, Redis 중복처리, Airflow를 활용한 매일 데이터 수집-분석-시각화 파이프라인 실행, 이상탐지 모델 서빙 은 현실적인 문제(CAPCHA, 정답 데이터의 부재 등)로 인해 개발하지 못하기도 했고, 들어가는 공수를 고려하여 간단한 프로젝트 규모에 맞게 축소하여 진행하기도 하였습니다. (MessageQueue -> Request/Response, Redis 중복처리 -> 휴리스틱한 중복처리)

### Issues
작업 내용 대부분은 이슈에 정리해두었습니다.

- [LoL - 게임 데이터 분석 프로젝트 기획](https://github.com/uowol/Game-Data-Analysis/issues/3) 
  - 시작은 창대했지만 일부만 구현하고 또 많이 바뀌었습니다. 
- [LoL - ERD 테이블 설계](https://github.com/uowol/Game-Data-Analysis/issues/4) 
  - 못난 ERD지만 프로젝트 시작을 위한 기반을 마련해주었습니다. 
- [LoL - PostgreSQL 데이터베이스 구현](https://github.com/uowol/Game-Data-Analysis/issues/5) 
  - 처음 SQL 프로젝트를 진행해보고 Metabase를 연동하여 대시보드를 만들어보았습니다.
- [LoL - Metabase를 활용한 대시보드 제작](https://github.com/uowol/Game-Data-Analysis/issues/6) 
  - PostgreSQL에서 시작하여 DuckDB로 옮겼습니다.
- [Common - DuckDB, MongoDB(OR Redis), Azure Data Explorer, BigQuery 사용 후기 작성](https://github.com/uowol/Game-Data-Analysis/issues/7) 
  - 다양한 DB, DWH, 로컬, 클라우드 여러 환경을 다뤄보면서 후기를 남겨두었습니다.
- [LoL - 랭크, 일반, 칼바람, 아레나 등 모든 게임 모드의 데이터를 수집하는 오류](https://github.com/uowol/Game-Data-Analysis/issues/8)
  - 원래 계획은 랭크 데이터만 수집하는 것이었는데 오히려 좋다는 마인드로 비즈니스 지표를 더 확장적으로 설정해보았습니다.
- [LoL - 비즈니스 지표 설정](https://github.com/uowol/Game-Data-Analysis/issues/9)
  - '포괄적인 문제 정의’ → ‘정량지표 설정’ → ‘지표 구체화’ → ‘관찰‘ → ‘문제 상황파악 및 가설설정’ → ‘접근지점 파악 및 지표설정’ → ‘결과 종합하여 결론내리기' 단계 중 ‘관찰‘ 단계까지 대시보드를 활용해 구현해보기로 계획했습니다.
- [LoL - 1차 데이터 수집 프로세스 완성](https://github.com/uowol/Game-Data-Analysis/issues/10)
  - 전체 분포를 고려하여 전체를 대표할 수 있는 데이터를 어떻게 추출할 수 있을까 고민해보았습니다.

### Commit Logs

그러나 정리할 힘이 남지 않아 파이프라인 개발 과정은 따로 정리하지 않았습니다.
아래와 같이 몇몇 커밋 로그에 적어둔 내용을 참고해주세요 (👉ﾟヮﾟ)👉

![image](https://github.com/user-attachments/assets/033195e0-a28c-4860-bc18-17522b730269)

