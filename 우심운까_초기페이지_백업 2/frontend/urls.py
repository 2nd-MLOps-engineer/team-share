from django.urls import path

from . import views
from .profile_demo import demo_recommendations
from .live_recommendations import live_recommendations


urlpatterns = [
    path("", views.welcome, name="welcome"),
    path("main/", views.index, name="index"),
    path("main/", views.index, name="home"),
    path("friends/", views.index, name="friends"),
    path("profile/", views.index, name="profile"),
    path("login/", views.login_page, name="login"),
    path("signup/", views.signup_page, name="signup"),
    path("api/check-member-nickname/", views.check_member_nickname, name="check_member_nickname"),
    path("profile/clear/", views.clear_profile, name="profile_clear"),
    path("api/check-nickname/", views.check_nickname, name="check_nickname"),

    # 추천 페이지를 데이터 원천별로 완전히 분리
    path("recommend/home/", views.recommend_home, name="recommend_home"),
    path("recommend/custom/", views.recommend_custom, name="recommend_custom"),

    # 예전 링크 호환용: /recommend/ -> /recommend/home/
    path("recommend/", views.recommend, name="recommend"),

    path("demo-api/recommendations/", demo_recommendations, name="profile_demo_recommendations"),
    path("api/live-recommendations/", live_recommendations, name="live_recommendations"),
    path("local/", views.local_consumption, name="local_consumption"),
]
