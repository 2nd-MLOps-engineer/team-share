import requests

url = (
    "https://www.bigdata-culture.kr/bigdata/user/data_market/process.file.do"
    "?type=distribution"
    "&re_id=b5880ea0-247a-4258-9f7b-79eab6751591"
    "&title=%EC%A0%84%EA%B5%AD%EA%B3%B5%EA%B3%B5%EC%B2%B4%EC%9C%A1%EC%8B%9C%EC%84%A4%20%EB%8D%B0%EC%9D%B4%ED%84%B0(202607)"
    "&file_id=a979eea4-ddea-4e5c-a26d-76a676184741"
    "&use_name=%EA%B8%B0%ED%83%80(%EC%9E%91%EC%84%B1:%20%20%20)%20"
    "&use_common_code=004001004"
)

response = requests.get(url, timeout=30)

print(response.status_code)
print(response.headers.get("Content-Type"))
print(response.headers.get("Content-Disposition"))

with open("facility.csv", "wb") as f:
    f.write(response.content)

print("다운로드 완료:", len(response.content), "bytes")
print(response.text[:1000])
print(response.url)
print(response.history)