from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import time

def main():
    # Chrome 옵션 설정
    options = Options()

    # 사용자 환경과 유사하게 브라우저 옵션 설정
    options.add_argument("--start-maximized")
    options.add_argument("--disable-infobars")  # 정보 표시창 제거 (자동화 감지 메시지 방지)
    options.add_argument("--disable-blink-features=AutomationControlled")  # 자동화 감지 기능 비활성화

    # 탐지 우회: Chrome 내부 플래그 설정 변경
    options.add_experimental_option("excludeSwitches", ["enable-automation"])  # 자동화 플래그 제거
    options.add_experimental_option("useAutomationExtension", False)  # 셀레니움 확장 비활성화

    # User-Agent 변경 (기본 user-agent는 봇으로 감지될 수 있음)
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )

    # 드라이버 실행
    driver = webdriver.Chrome(options=options)

    # # 자바스크립트 실행: `navigator.webdriver` 제거
    # # 많은 봇 탐지 시스템은 이 속성이 `true`인 걸로 셀레니움을 감지함
    # driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
    #     "source": """
    #         Object.defineProperty(navigator, 'webdriver', {
    #             get: () => undefined
    #         });
    #         window.navigator.chrome = {
    #             runtime: {}
    #         };
    #         Object.defineProperty(navigator, 'languages', {
    #             get: () => ['en-US', 'en']
    #         });
    #         Object.defineProperty(navigator, 'plugins', {
    #             get: () => [1, 2, 3, 4, 5]
    #         });
    #     """
    # })

    # 페이지 접속 (예: 봇 탐지 여부 확인용)
    driver.get("https://bot.sannysoft.com/")  # 봇 탐지 여부 보여주는 사이트

    # 잠시 대기 후 종료
    time.sleep(10)
    driver.quit()

if __name__ == "__main__":
    main()