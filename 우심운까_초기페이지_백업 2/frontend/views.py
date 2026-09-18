from django.shortcuts import redirect, render
from django.http import JsonResponse
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_http_methods, require_POST
from django.contrib.auth.hashers import check_password, make_password

from .forms import LoginForm, ProfileStartForm, SignupForm
from .models import Member, Profile
from .profile_demo import PROFILE_SESSION_KEY, get_saved_profile


def welcome(request):
    return render(request, "pages/welcome.html")


@never_cache
def index(request):
    """PAGE 1 - 메인 / 지역 랭킹 화면."""
    profile = get_saved_profile(request)

    return render(
        request,
        "pages/home.html",
        {
            "profile": profile,
            "has_profile": bool(profile),
        },
    )


@never_cache
@require_http_methods(["GET", "POST"])
def login_page(request):
    """닉네임과 비밀번호로 회원을 확인하고 메인으로 이동한다."""
    error = None
    if request.method == "POST":
        form = LoginForm(request.POST)
        if form.is_valid():
            member = Member.objects.filter(nickname=form.cleaned_data["nickname"]).first()
            if member is None:
                error = "아이디 또는 비밀번호 오류입니다."
            elif not check_password(form.cleaned_data["password"], member.password_hash):
                error = "아이디 또는 비밀번호 오류입니다."
            else:
                request.session["member_id"] = member.pk
                request.session["member_nickname"] = member.nickname
                request.session.set_expiry(0)
                return redirect("index")
    else:
        form = LoginForm()

    return render(
        request,
        "pages/login.html",
        {
            "form": form,
            "error": error,
        },
    )


@never_cache
def signup_page(request):
    if request.method == "POST":
        form = SignupForm(request.POST)
        if form.is_valid():
            member = Member.objects.create(
                name=form.cleaned_data["name"],
                nickname=form.cleaned_data["nickname"],
                password_hash=make_password(form.cleaned_data["password"]),
                address=form.cleaned_data["address"],
            )
            return redirect("login")
    else:
        form = SignupForm()
    return render(request, "pages/signup.html", {"form": form})


@never_cache
@require_http_methods(["GET"])
def check_member_nickname(request):
    nickname = request.GET.get("nickname", "").strip()
    return JsonResponse({"available": bool(nickname) and not Member.objects.filter(nickname=nickname).exists()})


@never_cache
def recommend(request):
    """기존 /recommend/ 주소 호환용. 내 지역 추천 화면으로 보낸다."""
    return redirect("recommend_home")


@never_cache
def recommend_home(request):
    """
    내 지역 추천 전용 화면.

    데이터 원천은 로그인 세션의 profile.city 하나만 사용한다.
    다른 지역 선택값(target_region)은 이 view에서 읽지 않는다.
    """
    profile = get_saved_profile(request)

    if profile is None:
        return redirect("login")

    selected_region = str(profile.get("city", "")).strip()

    if not selected_region:
        return redirect("login")

    return render(
        request,
        "frontend/recommend_home.html",
        {
            "profile": profile,
            "selected_region": selected_region,
            "recommend_mode": "home",
        },
    )


@never_cache
def recommend_custom(request):
    """
    다른 지역 추천 전용 화면.

    데이터 원천은 이 요청의 target_region query parameter만 사용한다.
    profile.city는 사용자 표시용으로만 남고 추천 지역을 덮어쓰지 않는다.
    """
    profile = get_saved_profile(request)

    if profile is None:
        return redirect("login")

    selected_region = request.GET.get("target_region", "").strip()

    if not selected_region:
        return redirect("index")

    return render(
        request,
        "frontend/recommend_custom.html",
        {
            "profile": profile,
            "selected_region": selected_region,
            "recommend_mode": "custom",
        },
    )


@never_cache
@require_POST
def clear_profile(request):
    """저장된 프로필 삭제 후 로그인으로 이동."""
    profile_id = request.session.get("profile_id")
    if profile_id:
        Profile.objects.filter(pk=profile_id).delete()
    request.session.pop("profile_id", None)
    request.session.pop(PROFILE_SESSION_KEY, None)
    return redirect("login")


@never_cache
def check_nickname(request):
    nickname = request.GET.get("nickname", "").strip()
    current_id = request.session.get("profile_id")
    query = Profile.objects.filter(nickname=nickname)
    if current_id:
        query = query.exclude(pk=current_id)
    return JsonResponse({"available": bool(nickname) and not query.exists()})


@never_cache
def local_consumption(request):
    """운동 후 지역 소비 페이지."""
    profile = get_saved_profile(request)

    return render(
        request,
        "frontend/local_consumption.html",
        {
            "profile": profile,
        },
    )
