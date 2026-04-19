# 하나투어 광고 EP 반자동 오케스트레이션 V2

## 프로젝트 개요

기존 하나투어 광고 EP 자동화 시스템은 광고 생성과 운영을 간편하게 만들어주었지만, 완전 자동화 방식의 한계도 존재했다. 상품 상태, 하나투어 웹 API 장애, 예약 가능 여부, 좌석 수 변화 등에 따라 상품 노출 여부가 자주 달라졌고, 동일한 상품 코드에 대해서도 제목과 광고 ID, 이미지가 지속적으로 변경되는 문제가 발생했다.

특히 하나의 패턴 번호에 대해 15~20개의 상품이 지속적으로 교체되면서 기존 광고 ID와 광고 제목이 불규칙하게 변동되었고, 이는 네이버 지식쇼핑 광고의 고정 노출에 부정적인 영향을 주었다. 광고가 안정적으로 노출되지 못하고 랜덤하게 변동되면서 광고 효율 또한 떨어지는 문제가 있었다.

이를 해결하기 위해 기존의 완전 자동화 방식에서 벗어나, 광고 ID·제목·이미지 등을 사람이 직접 관리할 수 있는 반자동 구조로 개선하였다.  
기존 자동화 시스템이 생성해둔 제목 데이터는 그대로 활용하되, 광고 운영자가 직접 상품 코드별 제목과 이미지를 선택하고 고정적으로 사용할 수 있도록 변경하였다.

---

## 주요 개선 사항

- 기존 자동 생성 제목 데이터를 기반으로 상품 코드별 제목을 직접 지정하고 고정적으로 사용할 수 있도록 개선
- 광고 이미지 또한 자동 수집이 아닌 운영자가 직접 선택할 수 있도록 하여, 중복 이미지를 최소화하고 다양한 광고 구성이 가능하도록 개선
- 상품 상태 이상, 중복 상품, 품절, 좌석 부족, API 장애 등의 문제를 실시간으로 탐지
- 모든 오류 상품 코드는 Slack 알림과 클라우드 로그를 통해 즉시 확인 가능
- 문제가 발생한 상품은 자동으로 EP 결과물에서 제외하여 잘못된 광고 노출을 방지
- 기존 완전 자동화 대비 API 호출 수를 최소화하고, 필요한 엔드포인트만 사용하도록 최적화
- 광고 운영자가 상품별로 특정 EP 컬럼을 직접 수정 및 커스터마이징할 수 있도록 개선
- 기존 자동 생성 제목뿐 아니라 실제 상위 노출 중인 광고 제목 패턴을 반영하여 더욱 효율적인 제목 생성 가능

---

## 오케스트레이션 플로우

### 1. 광고 템플릿 데이터 로드
브론즈 버킷에 저장된 광고 템플릿 데이터를 읽는다.

광고 템플릿에는 아래와 같은 고정 정보가 포함된다.

- 광고 ID
- 상품 코드
- 광고 제목
- 상품 이미지
- 기타 EP 고정 컬럼 정보

### 2. 상품 상태 1차 검증
광고 템플릿에 포함된 상품 코드 기준으로 예약 가능 여부를 우선 확인한다.

검증 항목은 다음과 같다.

- 품절 여부
- 좌석 수 부족 여부
- 예약 가능 여부
- 상품 상태 이상 여부
- 하나투어 API 응답 가능 여부

이 과정에서 문제가 발생한 상품 코드는 XCom에 저장된다.

### 3. 가격 조회 및 1차 데이터셋 생성
사용 가능한 상품에 대해서만 가격 조회를 수행한다.

이후 상품 코드와 대분류 코드를 기준으로 1차 정제된 데이터셋을 생성하고, 이를 실버 버킷에 Parquet 형식으로 저장한다.

### 4. 리뷰 데이터 결합
월 1회 실행되는 전체 상품 리뷰 집계를 기반으로, 브론즈 버킷에 저장된 여행사별 예약 코드 리스트를 읽어온다.

예약 코드를 기준으로 Left Join을 수행하여 사용 가능한 리뷰 데이터를 결합하고, 최종 EP 생성 시 아래 정보를 포함한다.

- 리뷰 수
- 평균 평점
- 상품별 리뷰 노출 정보

### 5. EP 생성 및 중복 제거
최종 EP 스키마에 맞춰 데이터를 생성한 뒤, 제목 중복 여부를 검사한다.

동일한 제목이 여러 번 생성될 경우 첫 번째 항목만 유지하고 나머지는 제거한다.  
제거된 상품 정보는 XCom에 저장하여 이후 Slack 알림 및 로그 데이터셋 생성에 활용한다.

### 6. 결과 업로드
최종 생성된 EP 결과물을 TSV 형식으로 변환하여 SFTP를 통해 Cafe24에 업로드한다.

동시에 최종 결과물은 클라우드 골드 버킷에도 저장된다.

### 7. Slack 알림 및 로그 저장
전체 실행 과정 중 발생한 문제를 기반으로 성공/실패 Slack 알림을 전송한다.

알림에는 아래와 같은 정보가 포함된다.

- API 장애 상품 수
- 품절 상품 수
- 좌석 부족 상품 수
- 중복 제거 상품 수
- 비활성 상품 수
- 전체 업로드 성공 여부

또한 문제 상품 코드만 별도로 정리한 데이터셋을 생성하여 골드 버킷에 저장한다.

---

## 운영 관점에서의 장점

기존 완전 자동화 방식은 사람이 개입하지 않기 때문에 잘못된 상품이 광고로 노출되거나, 동일 상품이 반복 노출되는 등의 문제가 발생할 수 있었다.

반면 현재 반자동 구조는 광고 운영자가 직접 제목, 이미지, 일부 컬럼을 제어할 수 있기 때문에 광고 품질을 더욱 안정적으로 유지할 수 있다.

또한 상품 상태 이상이 발생할 경우 실행 직후 즉시 문제 상품 코드를 확인할 수 있어 빠른 대응이 가능하다.

문제가 있는 상품은 자동으로 EP 결과물에서 제외되므로 광고 품질 저하를 최소화할 수 있으며, 광고 등록 수량 감소 외에는 직접적인 손실이 발생하지 않도록 설계하였다.

---

## 프로젝트 발전 과정

초기 모델은 광고 상품과 랜딩 페이지의 연관성을 최대한 맞추고, 광고 제목과 상품 설명이 모두 연결되도록 하는 것에 집중하였다.

1차 모델에서는 상품별 상세 페이지와 광고를 1:1로 연결하는 구조를 완성하였고, 이 과정에서 수많은 제목 데이터가 축적되었다.

현재는 상품 가격이 매일 변동되는 특성을 고려하여, 짧은 주기로 최소한의 트래픽만 사용하여 가격 업데이트를 수행하는 방향으로 개선되었다.

특히 운영 중인 최근 2년치 예약 코드 데이터를 활용하여 상품별 리뷰 수와 평점을 집계할 수 있게 되었고, 이를 광고 EP에 반영함으로써 광고 신뢰도 향상에도 도움이 되고 있다.

향후 예약 코드 데이터가 지속적으로 누적될수록 리뷰 수와 평점 데이터 또한 더욱 정교해질 것으로 예상된다.

---

## 버전 2의 특징

자동화 버전 2는 기존 완전 자동화 모델에 사람의 개입을 추가한 반자동 모델이다.

기존 버전 1에서 생성한 제목과 이미지 자산을 재사용할 수 있도록 개선되었으며, 운영자가 보다 쉽게 광고를 관리할 수 있도록 설계되었다.

또한 버전 1 대비 아래와 같은 개선이 적용되었다.

- API 호출 최소화
- 특정 엔드포인트만 선택적으로 사용
- 실행 속도 향상
- 오케스트레이션 시간 단축
- 상세 로그 추가
- 문제 상품 즉시 추적 가능
- Slack 기반 실시간 장애 모니터링

---

## 향후 개선 방향

현재는 EC2 환경에서 운영 중이며, 메모리 사용량과 서비스 상태를 지속적으로 확인하고 있다.

향후 여유 자원이 확보되면 아래와 같은 오픈소스 모니터링 도구를 추가하여 더욱 안정적인 운영 환경을 구축할 예정이다.

- Grafana
- Prometheus
- Amazon EC2 기반 리소스 모니터링
- 로그 수집 및 장애 알림 시스템 고도화





# HanatourAdAutomation_v2

This repository focuses on minimizing API calls compared to the previous version. Rather than dynamically generating a wide variety of product titles, it emphasizes pre-generated titles and continuously updates product prices and review information.

The previous version relied heavily on full automation, which often caused frequent changes in advertisement IDs, titles, and images whenever product availability, API status, or reservation conditions changed. As a result, advertisements were not exposed consistently and could lose stability in external platforms such as Naver Knowledge Shopping.

Version 2 introduces a semi-automated approach. Product titles, advertisement IDs, and images can now be fixed and managed manually, while only frequently changing information such as prices, availability, seat status, and reviews are updated automatically.

## Key Features

- Minimized API calls by using only essential endpoints
- Pre-generated and reusable advertisement titles
- Fixed advertisement IDs and images for more stable exposure
- Automatic price updates for active products
- Automatic review count and rating updates
- Duplicate title detection and removal
- Product availability validation before processing
- Slack notifications for failures, duplicates, sold-out products, and API issues
- Cloud storage integration for Bronze, Silver, and Gold datasets
- SFTP upload support for Cafe24 EP deployment
- Detailed logging for easier troubleshooting and monitoring

## Workflow

1. Read advertisement template data from the Bronze bucket
2. Validate product availability, seat status, and reservation conditions
3. Request only required price information for available products
4. Save refined datasets into the Silver bucket
5. Merge product review and rating data based on reservation codes
6. Generate EP output and remove duplicated titles
7. Upload final TSV results to Cafe24 through SFTP
8. Store final outputs and error datasets in the Gold bucket
9. Send Slack notifications with summarized execution results

## Improvements Over Previous Version

- Reduced traffic by minimizing unnecessary API calls
- Stable advertisement exposure through fixed titles and images
- Easier manual control for advertisement operators
- Faster execution time and orchestration flow
- More detailed error tracking and monitoring
- Better support for long-term review accumulation and product ranking optimization

Future improvements may include infrastructure monitoring with Grafana and resource tracking on Amazon EC2 environments.