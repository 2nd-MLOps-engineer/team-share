from django import forms
import re
from .models import Member, Profile


REGION_ALIASES = {
    "수원": "경기도 수원시", "수원시": "경기도 수원시",
    "용인": "경기도 용인시", "용인시": "경기도 용인시",
    "성남": "경기도 성남시", "성남시": "경기도 성남시",
    "고양": "경기도 고양시", "고양시": "경기도 고양시",
    "부천": "경기도 부천시", "부천시": "경기도 부천시",
    "안양": "경기도 안양시", "안양시": "경기도 안양시",
    "화성": "경기도 화성시", "화성시": "경기도 화성시",
    "서울": "서울특별시", "인천": "인천광역시", "대전": "대전광역시",
    "대구": "대구광역시", "부산": "부산광역시", "울산": "울산광역시",
    "광주": "광주광역시", "제주": "제주특별자치도",
}
VALID_PROVINCES = ("서울특별시", "경기도", "인천광역시", "강원특별자치도", "충청북도", "충청남도", "대전광역시", "세종특별자치시", "전북특별자치도", "전라남도", "광주광역시", "대구광역시", "경상북도", "경상남도", "울산광역시", "부산광역시", "제주특별자치도")


def normalize_city(value):
    value = " ".join(value.strip().split())
    if value in REGION_ALIASES:
        return REGION_ALIASES[value]
    parts = value.split()
    if len(parts) >= 2 and parts[0] in REGION_ALIASES and REGION_ALIASES[parts[0]] in VALID_PROVINCES:
        value = " ".join([REGION_ALIASES[parts[0]], *parts[1:]])
    if value and not value.startswith(VALID_PROVINCES):
        return ""
    return value


class ProfileStartForm(forms.Form):
    """우심운까 간편 프로필 입력 폼."""

    AGE_GROUP_CHOICES = [
        ("10대", "10대"),
        ("20대", "20대"),
        ("30대", "30대"),
        ("40대", "40대"),
        ("50대", "50대"),
        ("60대 이상", "60대 이상"),
    ]

    nickname = forms.CharField(
        label="닉네임",
        max_length=20,
        strip=True,
    )

    age_group = forms.ChoiceField(
        label="나이대",
        choices=AGE_GROUP_CHOICES,
    )

    city = forms.CharField(
        label="사는 지역",
        max_length=40,
        strip=True,
    )

    def clean_nickname(self):
        nickname = self.cleaned_data["nickname"].strip()
        if "<" in nickname or ">" in nickname:
            raise forms.ValidationError("닉네임에는 꺾쇠 기호를 사용할 수 없습니다.")
        return nickname

    def clean_city(self):
        city = normalize_city(self.cleaned_data["city"])
        parts = city.split()
        if len(parts) < 2 or not parts[-1].endswith(("시", "군", "구")):
            raise forms.ValidationError("수원시처럼 시·군만 입력해도 되지만, 인식 가능한 지역명을 입력해주세요.")
        if "<" in city or ">" in city:
            raise forms.ValidationError("지역명에는 꺾쇠 기호를 사용할 수 없습니다.")
        return city


class SignupForm(forms.Form):
    name = forms.CharField(label="이름", max_length=50, strip=True)
    nickname = forms.CharField(label="닉네임", max_length=20, strip=True)
    password = forms.CharField(label="비밀번호", min_length=8, max_length=128, widget=forms.PasswordInput)
    password_confirm = forms.CharField(label="비밀번호 확인", min_length=8, max_length=128, widget=forms.PasswordInput)
    address = forms.CharField(label="주소", max_length=200, strip=True)

    def clean_name(self):
        value = self.cleaned_data["name"].strip()
        if not value:
            raise forms.ValidationError("이름을 입력해주세요.")
        return value

    def clean_nickname(self):
        value = self.cleaned_data["nickname"].strip()
        if "<" in value or ">" in value:
            raise forms.ValidationError("닉네임에는 꺾쇠 기호를 사용할 수 없습니다.")
        if Member.objects.filter(nickname=value).exists():
            raise forms.ValidationError("이미 사용 중인 닉네임입니다.")
        return value

    def clean(self):
        cleaned = super().clean()
        password = cleaned.get("password", "")
        if password and any(char.isspace() for char in password):
            self.add_error("password", "비밀번호에는 공백을 사용할 수 없습니다.")
        if password and re.search(r"[가-힣]", password):
            self.add_error("password", "비밀번호에는 한글을 사용할 수 없습니다.")
        if password and password.isdigit():
            self.add_error("password", "숫자만으로 이루어진 비밀번호는 사용할 수 없습니다.")
        if password and re.search(r"(.)\1{3,}", password):
            self.add_error("password", "같은 문자를 4번 이상 반복할 수 없습니다.")
        personal_values = {
            "".join(str(cleaned.get(key, "")).lower().split())
            for key in ("name", "nickname", "address")
        }
        normalized_password = "".join(password.lower().split())
        if normalized_password and normalized_password in personal_values:
            self.add_error("password", "이름·닉네임·주소만으로 된 비밀번호는 사용할 수 없습니다.")
        if password and cleaned.get("password_confirm") and password != cleaned["password_confirm"]:
            self.add_error("password_confirm", "비밀번호가 일치하지 않습니다.")
        return cleaned

    def clean_address(self):
        value = self.cleaned_data["address"].strip()
        if not value:
            raise forms.ValidationError("주소를 입력해주세요.")
        return value


class LoginForm(forms.Form):
    nickname = forms.CharField(label="닉네임", max_length=20, strip=True)
    password = forms.CharField(label="비밀번호", max_length=128, widget=forms.PasswordInput)
