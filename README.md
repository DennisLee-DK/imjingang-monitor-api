# 임진강 수문 감시 API

GitHub Pages 화면에 실시간 수위·기상·CCTV 자료를 제공하는 서버입니다.

## Render 환경 변수

Render 대시보드에서 다음 값을 **비밀 환경 변수**로 설정합니다.

- `HRFCO_API_KEY`
- `PUBLIC_DATA_SERVICE_KEY`
- `ITS_CCTV_API_KEY`
- `KMA_API_KEY`

이 저장소에는 실제 키나 `config.json`을 올리지 않습니다. 배포 완료 후
`https://<서비스명>.onrender.com/api/monitor`가 공개 화면의 데이터 주소입니다.
