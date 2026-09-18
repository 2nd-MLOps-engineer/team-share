(() => {
  const form = document.querySelector("#signupForm");
  const nickname = document.querySelector("#nickname");
  const checkButton = document.querySelector("#checkNicknameButton");
  const nicknameMessage = document.querySelector("#nicknameMessage");
  const password = document.querySelector("#password");
  const passwordConfirm = document.querySelector("#passwordConfirm");
  const passwordMessage = document.querySelector("#passwordMessage");
  let nicknameChecked = false;

  checkButton.addEventListener("click", async () => {
    const value = nickname.value.trim();
    if (!value) { nicknameMessage.textContent = "닉네임을 입력해주세요."; nicknameMessage.className = "field-message is-error"; return; }
    const response = await fetch(`/api/check-member-nickname/?nickname=${encodeURIComponent(value)}`);
    const data = await response.json();
    nicknameChecked = data.available;
    nicknameMessage.textContent = data.available ? "사용 가능한 닉네임입니다." : "이미 사용 중인 닉네임입니다.";
    nicknameMessage.className = data.available ? "field-message is-ok" : "field-message is-error";
  });

  nickname.addEventListener("input", () => { nicknameChecked = false; nicknameMessage.textContent = "닉네임 중복 확인이 필요합니다."; nicknameMessage.className = "field-message"; });
  const checkPasswords = () => { passwordMessage.textContent = passwordConfirm.value && password.value === passwordConfirm.value ? "비밀번호가 일치합니다." : "비밀번호를 다시 확인해주세요."; passwordMessage.className = passwordConfirm.value && password.value === passwordConfirm.value ? "field-message is-ok" : "field-message is-error"; };
  password.addEventListener("input", checkPasswords); passwordConfirm.addEventListener("input", checkPasswords);
  form.addEventListener("submit", event => { if (!nicknameChecked) { event.preventDefault(); nicknameMessage.textContent = "닉네임 중복 확인을 먼저 해주세요."; nicknameMessage.className = "field-message is-error"; } });
})();
