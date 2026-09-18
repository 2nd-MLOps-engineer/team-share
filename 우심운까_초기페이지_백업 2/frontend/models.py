from django.db import models


class Profile(models.Model):
    nickname = models.CharField(max_length=20, unique=True)
    age_group = models.CharField(max_length=20)
    city = models.CharField(max_length=40)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["nickname"]

    def __str__(self):
        return f"{self.nickname} ({self.city})"


class Member(models.Model):
    """회원가입 정보. 비밀번호는 password_hash에 해시로 저장한다."""

    name = models.CharField(max_length=50)
    nickname = models.CharField(max_length=20, unique=True)
    password_hash = models.CharField(max_length=128)
    address = models.CharField(max_length=200)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        ordering = ["-created_at"]

    def __str__(self):
        return f"{self.nickname} ({self.name})"
