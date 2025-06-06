# 📌 LANssenger

## 👥 Contributors

- [**전태현**](https://github.com/taehyunjeon0203) -
- [**유병연**](https://github.com/youbyeongyeon) -
- [**이유경**](https://github.com/dldbrud) -

## 🎯 주제

- LAN 대역에서 사용 가능한 네트워크 메신저

### 🛠 세부기능

- 기본 틀
  - 같은 네트워크 (ex: eduroam || KNU WIFI 6) 연결된 사용자들끼리 채팅 가능
- 세부 기능
  - 최초 접속시 닉네임 생성
  - 전체 채팅방 및 개별 채팅방 생성 기능
  - 방 이름을 정한 후 방을 생성하면 6자리 숫자가 할당, 원한다면 4자리 비밀번호 설정 가능
  - 방 설정: 방 이름 변경 및 나가기, 접속중인 사용자 초대, 채팅 차단
  - 현재 활동중인 인원 표시
  - 닉네임 형식(태현.xxx) <- .xxx == ip의 일부(지정 불가능)

## 📁 프로젝트 구조

```
lanssenger/
├── include/
│   ├── client/
│   │   ├── gui/           # GUI 관련 헤더
│   │   ├── chat_client.hpp
│   │   ├── chat_manager.hpp
│   │   ├── chat_message.hpp
│   │   └── nickname_manager.hpp
│   ├── server/
│   └── common/
├── src/
│   ├── client/
│   │   ├── gui/           # GUI 관련 소스
│   │   ├── chat_client.cpp
│   │   ├── chat_manager.cpp
│   │   ├── chat_message.cpp
│   │   └── nickname_manager.cpp
│   ├── server/
│   └── common/
└── scripts/               # 빌드 스크립트
```

## 🛠 빌드 방법

### 요구사항

- CMake 3.10 이상
- Qt6
- Boost
- C++17 지원 컴파일러

### 빌드

1. 클라이언트 빌드:

```bash
./scripts/build_client.sh  # Linux/macOS
scripts/build_client.bat   # Windows
```

2. 서버 빌드:

```bash
./scripts/build_server.sh  # Linux/macOS
scripts/build_server.bat   # Windows
```

## 📝 계획서(완료)

한글로 작성

1. 개요 \*_구축하려는 서비스에 대한 소개 (아래는 예시)_  
   ① 제공하고자 하는 서비스가 무엇인지?  
   ② 서비스의 사용자는 누구인지?  
   ③ 사용자가 서비스를 사용함으로써 얻을 수 있는 효과는 무엇인지?  
   ④ 기존에 존재하는 서비스들과의 차별점은 무엇인지?
2. 향후 개발 계획  
   ① 개발 관련 계획 (협업 방법, 개발 도구 등)  
   ② 향후 일정  
   ③ 팀원별 업무 분담 계획

## 📅 일정

- 중간
  - 발표일 (5월 2일 금요일) // 상황에 따라 1주 정도 빨라질 가능성 있음
  - 프로젝트 계획서 제출일 (5월 1일 화요일 13시) // hwp로 작성, Email로 제출
- 기말
  - 프로젝트 구현 및 발표
