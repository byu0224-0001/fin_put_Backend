from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import sessionmaker, declarative_base
from app.config import settings
from neo4j import GraphDatabase
import logging

logger = logging.getLogger(__name__)

# PostgreSQL 설정
# Windows 환경에서 인코딩 문제 방지를 위해 URL.create()를 사용하여 개별 파라미터로 연결
# DSN 문자열 파싱 오류를 피하기 위해 SQLAlchemy URL 객체를 직접 생성
try:
    # 개별 파라미터를 사용하여 URL 객체 생성 (인코딩 문제 방지)
    # 이렇게 하면 psycopg2가 DSN 문자열을 파싱할 때 발생하는 인코딩 오류를 피할 수 있습니다
    # SQLAlchemy URL 객체를 사용하면 내부적으로 올바른 인코딩을 처리합니다
    database_url = URL.create(
        drivername="postgresql",
        username=settings.POSTGRES_USER,
        password=settings.POSTGRES_PASSWORD,
        host=settings.POSTGRES_HOST,
        port=int(settings.POSTGRES_PORT),
        database=settings.POSTGRES_DB,
        query={
            "client_encoding": "utf8"
        }
    )
    
    # 엔진 생성 시 connect_args에 인코딩 명시
    engine = create_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
        # psycopg2에 직접 전달되는 인자들
        connect_args={
            "options": "-c client_encoding=UTF8"  # PostgreSQL 서버에 인코딩 명시
        },
        echo=False,  # SQL 로그 출력 (디버그 시 True로 변경)
    )
    
    logger.info(f"PostgreSQL 엔진 생성 성공: {settings.POSTGRES_HOST}:{settings.POSTGRES_PORT}/{settings.POSTGRES_DB}")
    
except Exception as e:
    logger.error(f"PostgreSQL 엔진 생성 실패: {e}")
    import traceback
    logger.error(traceback.format_exc())
    logger.error(f"연결 정보: host={settings.POSTGRES_HOST}, port={settings.POSTGRES_PORT}, db={settings.POSTGRES_DB}, user={settings.POSTGRES_USER}")
    # 연결 실패 시에도 엔진을 생성하여 오류 메시지를 명확하게 표시
    raise

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# SQLAlchemy Base 클래스
Base = declarative_base()


# Neo4j 연결
class Neo4jDriver:
    """Neo4j 연결 관리 클래스"""
    
    def __init__(self):
        self._driver = None
        self._uri = settings.NEO4J_URI
        self._user = settings.NEO4J_USER
        self._password = settings.NEO4J_PASSWORD
    
    def connect(self):
        """Neo4j 드라이버 연결"""
        try:
            self._driver = GraphDatabase.driver(
                self._uri,
                auth=(self._user, self._password)
            )
            logger.info("Neo4j 연결 성공")
            return self._driver
        except Exception as e:
            logger.error(f"Neo4j 연결 실패: {e}")
            raise
    
    def get_driver(self):
        """드라이버 인스턴스 반환"""
        if self._driver is None:
            self.connect()
        return self._driver
    
    def close(self):
        """드라이버 연결 종료"""
        if self._driver:
            self._driver.close()
            logger.info("Neo4j 연결 종료")


# 전역 Neo4j 드라이버 인스턴스
neo4j_driver = Neo4jDriver()


def get_db():
    """데이터베이스 세션 의존성"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def get_neo4j():
    """Neo4j 드라이버 의존성"""
    driver = neo4j_driver.get_driver()
    session = driver.session()
    try:
        yield session
    finally:
        session.close()

