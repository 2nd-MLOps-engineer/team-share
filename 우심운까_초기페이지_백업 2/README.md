# 우심운까 초기 페이지 백업

오프닝 페이지, 회원가입 페이지, 로그인 페이지와 해당 정적 파일을 모아둔 백업입니다.

## 포함된 화면

- `frontend/templates/pages/welcome.html`: 오프닝 화면
- `frontend/templates/pages/signup.html`: 회원가입 화면
- `frontend/templates/pages/login.html`: 로그인 화면
- `frontend/static/assets/css/`: 화면 공통·오프닝·회원가입 스타일
- `frontend/static/assets/js/signup.js`: 닉네임 중복 확인 및 비밀번호 일치 확인
- `frontend/static/assets/images/character/mobi_bike.png`: 자전거를 타며 손 흔드는 캐릭터

## 백엔드 연결 파일

`models.py`, `forms.py`, `views.py`, `urls.py`, `migrations/0002_member.py`에 회원가입 및 로그인 연결 코드가 포함되어 있습니다.

## 회원 테이블 백업

`frontend_member_table.sql`은 PostgreSQL의 `frontend_member` 테이블만 백업한 파일입니다. 현재 가입된 데이터와 테이블 구조가 함께 들어 있습니다. 비밀번호는 원문이 아니라 `pbkdf2_sha256` 해시로 저장되어 있습니다.

복원할 데이터베이스에 접속한 뒤 다음 명령을 실행합니다.

```bash
psql -h 127.0.0.1 -U backend_user -d woosimwoonkka -f frontend_member_table.sql
```

테이블이 이미 존재한다면 먼저 기존 테이블을 덮어쓸지 확인한 후 진행해야 합니다. 이 백업은 회원 테이블만 포함하며 전체 데이터베이스 백업이 아닙니다.

## 동일한 테이블을 새로 만들기

백업 파일을 사용하지 않는 경우 Django 프로젝트에서 다음 순서로 실행합니다.

```bash
python manage.py makemigrations frontend
python manage.py migrate
```

필요한 모델 구조는 `Member` 모델이며 다음 필드를 사용합니다.

| 필드 | 타입 | 설명 |
|---|---|---|
| `id` | bigint | 기본 키 |
| `name` | varchar(50) | 이름 |
| `nickname` | varchar(20) | 고유 닉네임 |
| `password_hash` | varchar(128) | PBKDF2-SHA256 해시 |
| `address` | varchar(200) | 주소 |
| `created_at` | timestamp | 가입 시각 |
| `updated_at` | timestamp | 수정 시각 |

## 실행

프로젝트 전체가 있는 경우 `우심운까 데모 실행.command`를 더블클릭하거나 다음 주소를 엽니다.

`http://127.0.0.1:8000/`
