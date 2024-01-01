# 사용할 Java 버전을 지정하는 베이스 이미지
FROM openjdk:17-jdk-alpine

RUN apk upgrade --no-cache && apk add --no-cache postgresql-client bash openssl libgcc libstdc++ ncurses-libs

# JAR 파일 경로 인자
ARG JAR_FILE=build/libs/cep-example-kafka-kstreams-1.0-SNAPSHOT.jar

# JAR 파일을 컨테이너 내부로 복사
COPY ${JAR_FILE} app.jar

# 컨테이너 실행 시 실행할 명령어
ENTRYPOINT ["java","-jar","/app.jar"]
