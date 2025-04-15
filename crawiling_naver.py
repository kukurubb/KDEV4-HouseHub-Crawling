from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, WebDriverException
)
import requests
import time
import json
import re
import math
import random
import os
import pandas as pd


"""
지금 여기 추가해야 하는 기능
1. 이미 매물이 사라져서 에러값이 들어오는 경우 삭제처리
2. 중복 매물 제거
"""


class NaverCrawler:
    def __init__(
        self,
        headless=False,
        min_time=2,
        max_time=10
    ):
        self.headless = headless
        self.min_time = min_time
        self.max_time = max_time

    def set_options(self):
        # Chrome 옵션 설정
        options = Options()

        # 사용자 환경과 유사하게 브라우저 옵션 설정
        options.add_argument("--start-maximized")
        options.add_argument('--headless') if self.headless else None # 화면 비활성화
        options.add_argument('--disable-gpu')  # GPU 가속 비활성화 (Windows에서 필수일 수 있음)
        options.add_argument("--disable-infobars")  # 정보 표시창 제거 (자동화 감지 메시지 방지)
        options.add_argument("--disable-blink-features=AutomationControlled")  # 자동화 감지 기능 비활성화
        options.set_capability("goog:loggingPrefs", {"performance": "ALL"}) # DevTools 로그 수집을 위한 설정 / XHR 추적용도

        # 탐지 우회: Chrome 내부 플래그 설정 변경
        options.add_experimental_option("excludeSwitches", ["enable-automation"])  # 자동화 플래그 제거
        options.add_experimental_option("useAutomationExtension", False)  # 셀레니움 확장 비활성화

        # User-Agent 변경
        options.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/120.0.0.0 Safari/537.36"
        )

        return options

    def set_headers(self):
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/120.0.0.0 Safari/537.36",

            "Referer": "https://m.land.naver.com/",  # 네이버 내부 요청처럼 위장

            "X-Requested-With": "XMLHttpRequest",    # AJAX 요청처럼 보이게

            "Accept": "application/json, text/javascript, */*; q=0.01",  # JSON 응답 허용

            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7"     # 한국어 브라우저 설정 흉내
        }
        return headers
        
    def open_sidebar(self, driver, wait):
        try:
            # 매물 목록 버튼 대기
            element = wait.until(EC.presence_of_element_located((By.CLASS_NAME, "btns_fixed_inner")))

            try:
                # 매물 목록 버튼 클릭
                article_button = element.find_element(By.CSS_SELECTOR, ".btn_option._article")

                # 버튼이 보이지 않을 경우 display 속성 강제로 block 설정
                if not article_button.is_displayed():
                    element.execute_script("arguments[0].style.display = 'block';", article_button)
                    WebDriverWait(driver, 5).until(EC.visibility_of(article_button)) # 렌더링 대기

                article_button.click()

            except NoSuchElementException:
                print("매물 목록 버튼을 찾을 수 없습니다.")
            
        except TimeoutException:
            print("매물 목록 버튼이 대기시간 안에 나타나지 않았습니다.")
        except WebDriverException as e:
            print(f"브라우저 오류 또는 네트워크 문제 발생: {e}")

    def scroll_down_sidebar(self, driver, wait, num_scroll_down):
        # article_box 요소 찾기
        article_box = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ".article_box.article_box--sale")))

        # n회 스크롤 내리기 수행
        for _ in range(num_scroll_down):
            driver.execute_script("arguments[0].scrollTop += 500;", article_box)
            time.sleep(0.5)

    """
    page_list 생성 시 필요한 url을 받기 위한 코드
    사이드바에서 스크롤 다운 시 매물 목록을 페이지네이션 방식으로 업데이트 함
    매물은 20개씩 업데이트되며 firefox 개발자도구 -> XHR -> articleList?itemId= 검색을 통해 요청 확인이 가능
    해당 요청을 확인하면 response 내부에는 매물 id 값이 들어있음
    그리고 해당 함수는 이 요청에 대한 url을 얻기 위한 코드임
    url 예시:
        https://m.land.naver.com/cluster/ajax/articleList?itemId=&mapKey=&lgeo=&showR0=&
        rletTpCd=APT:OPST:VL:YR:DSD:ABYG:OBYG:JGC:JWJT:DDDGG:SGJT:HOJT:JGB:OR:GSW:SG:SMS:GJCG:GM:TJ:APTHGJ& <- 매물 종류 코드 설정
        tradTpCd=A1:B1:B2:B3&
        z=14& <- 화면 view 사이즈
        lat=37.5882&lon=127.0464&btm=37.5623844&lft=126.9731435&top=37.6140067&rgt=127.1196565& <- 현재 조회 구역의 위도 경도 설정
        totCnt=83046& <- 전체 매물 수를 20으로 나누면 페이지 개수를 알 수 있음
        cortarNo=&sort=rank&
        page=2 <- 이 페이지 번호만 변경하면서 계속 매물 id를 얻을 수 있음
    """
    def get_xhr_url(self, driver):
        # XHR 로그 수집
        logs = driver.get_log("performance")

        # articleList 관련 요청 추출
        for entry in logs:
            log = json.loads(entry["message"])["message"]
            if (
                log.get("method") == "Network.requestWillBeSent"
                and "request" in log["params"]
                and "url" in log["params"]["request"]
            ):
                url = log["params"]["request"]["url"]
                if "articleList?itemId=" in url and "page=2" in url:
                    print("[XHR URL]\n", url)
                    formatted_url = url.replace("page=2", "page={}")
                    return formatted_url

    # 전체 매물 개수를 20으로 나눠 전체 페이지 수를 계산하는 함수    
    def get_num_pages(self, url):
        # totCnt 값 추출
        match = re.search(r"totCnt=(\d+)", url)
        tot_cnt = int(match.group(1))
        num_pages = math.ceil(tot_cnt / 20) # 한번에 20개의 매물을 보여줌

        return num_pages
    
    def make_dir(self, folder_path):
        if not os.path.exists(folder_path):
            # 폴더가 없는 경우 생성
            os.makedirs(folder_path)
            print(f"폴더를 생성했습니다.: {folder_path}")
        else:
            # 폴더가 존재하는 경우 생성x
            print(f"폴더가 이미 존재합니다.: {folder_path}")

    # id_list, page_list, property_list 저장 폴더 생성
    def create_csv(self, dir, csv_path, columns):
        # 폴더 생성
        self.make_dir(dir)

        # csv 파일 생성
        if not os.path.exists(csv_path):
            df = pd.DataFrame(columns=columns) # 빈 데이터프레임 생성
            df.to_csv(csv_path, index=False)
            print(f"csv 파일을 생성했습니다.: {csv_path}")
        else:
            print(f"csv 파일이 이미 존재합니다.: {csv_path}")

    # 매물 저장 시 성공/실패/에러 코드 기록
    # 크롤링 재수행 시 실패/에러 코드인 매물에 대해 크롤링 재수행
    def update_status(self, status, column_name, value, csv_path):
        df = pd.read_csv(csv_path)
        df.loc[df[f"{column_name}"] == value, "status"] = status # 데이터 조회 후 저장
        df.to_csv(csv_path, index=False)

    # 매물 id 기록
    def write_id_list(self, item_ids, csv_path):
        # 속도 향상을 위해 pandas 대신 txt 기록 방식 선정
        with open(csv_path, "a", encoding="utf-8") as f: # a: append
            for id in item_ids:
                text = f"{id},\n" # status는 기록하지 않음
                f.write(text)

    def random_time_sleep(self, min_time, max_time):
        # 랜덤 딜레이
        delay_time = random.randint(min_time, max_time)
        time.sleep(delay_time)

        
    def save_property_datail(self, data, item_id, save_dir):
        txt_path = os.path.join(save_dir, f"{item_id}" + ".txt")
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write(json.dumps(data, indent=2, ensure_ascii=False))

    # cURL 추출 함수
    # 브라우저 로그에서 API 요청 추출 후 cURL 생성
    """
    네이버의 경우 매물 상세 정보에 접근 시 url 이외에도 header 및 cookie 정보 등을 동시에 요청
    이 때 이러한 정보들을 종합하여 웹사이트에 접속한 agent가 봇인지 판단하여 차단
    혹은 매물 상세 정보에 접근 시의 cookie, header 정보가
    주소를 생성할 때의 header, cookie 정보와 다르면 차단
    따라서 curl을 통해 header, cookie 정보 등을 획득해야 함
    """
    def extract_curl_from_log(self, driver):
        logs = driver.get_log("performance")  # 성능 로그 수집

        for entry in logs:
            log = json.loads(entry["message"])["message"]

            # 네트워크 요청 중 API 호출만 필터링
            if (
                log.get("method") == "Network.requestWillBeSent"
                and "request" in log["params"]
                and "url" in log["params"]["request"]
            ):
                req = log["params"]["request"]
                url = req["url"]
                headers = req.get("headers", {})
                method = req.get("method", "GET")

                # 타겟 API 요청 조건: articles + complexNo 포함
                if "/api/articles/" in url and "complexNo=" in url:
                    # cURL 생성
                    _headers = {}
                    for key, value in headers.items():
                        _headers[key] = value
                    
                    # cURL 생성 성공
                    return _headers
        
        # cURL 생성 실패
        return None

    def crawl_item_ids(self, data_dir, area_id, lat, lon, view):
        """
        crawled_data/
        │   # 해당 지역의 매물 리스트 페이지 번호와 수신 성공 여부 기록
        ├── page_list/
        │   ├── area1.csv
        │   ├── area2.csv
        │   └── area3.csv
        │   # 해당 지역의 매물 item_id와 수신 성공 여부 기록
        ├── id_list/
        │   ├── area1.csv
        │   ├── area2.csv
        │   └── area3.csv
        │   # 매물 상세정보 기록
        └── property_list/
            ├── 2500001.txt
            ├── 2500002.txt
            └── 2500003.txt
        """
        # 파일 생성
        page_list_dir = os.path.join(data_dir, "page_list")
        page_list_csv_path = os.path.join(page_list_dir, f"{area_id}.csv")
        id_list_dir = os.path.join(data_dir, "id_list")
        id_list_csv_path = os.path.join(id_list_dir, f"{area_id}.csv")

        self.create_csv(
            dir=page_list_dir,
            csv_path=page_list_csv_path,
            columns=["page", "status"],
        )
        self.create_csv(
            dir=id_list_dir,
            csv_path=id_list_csv_path,
            columns=["item_id", "status"],
        )

        # 옵션 설정
        headers = self.set_headers()
        options = self.set_options()

        # 크롬 브라우저 자동 실행
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        wait = WebDriverWait(driver, 10)

        # 접속 URL
        """
        APT:   아파트
        OPST:  오피스텔
        VL:    빌라
        JWJT:  전원주택
        DDDGG: 단독/다가구
        SGJT:  상가주택
        HOJT:  한옥주택
        OR:    원룸
        GSW:   고시원

        A1: 매매
        B1: 전세
        B2: 월세
        B3: 단기임대
        """
        url = f"https://m.land.naver.com/map/{lat}:{lon}:{view}/APT:OPST:VL:JWJT:DDDGG:SGJT:HOJT:OR:GSW/A1:B1:B2:B3#mapFullList"
        driver.get(url)

        # 사이드바 열기
        self.open_sidebar(driver, wait)

        # 사이드바 스크롤 다운 n회 수행
        self.scroll_down_sidebar(driver, wait, num_scroll_down=3)
        
        # XHR 주소 수집
        item_ids_url = self.get_xhr_url(driver)

        # XHR 주소로 접근하면서 매물 아이디 수집
        num_pages = self.get_num_pages(item_ids_url)

        """
        이 부분의 코드는 num_pages를 받아 기존 csv 파일의 page 번호를 덮어쓰는 방식임
        현재 코드의 문제점은 페이지에 포함된 매물 id 리스트는 고정된 것이 아님
        따라서 크롤링을 이어하는 경우 페이지 개수의 변화나 매물 id 값이 변동될 가능성이 있음
        다만 페이지 번호 크롤링의 경우 오래 걸리는 작업이 아니기 때문에 이런 문제점을
        무시하고 진행하는 것을 가정함
        그리고 페이지 번호만 덮어쓰기가 되고 성공/실패 여부는 그대로 남아있음
        """
        # 페이지 번호 기록
        page_list_df = pd.read_csv(page_list_csv_path)
        page_list_df["page"] = list(range(1, num_pages + 1))
        page_list_df.to_csv(page_list_csv_path, index=False)
        
        # item_id 크롤링
        """
        for i in range(1, num_pages + 1):
        페이지 번호를 순차적으로 증가시키는 방식은
        네이버에 의해 금방 차단당함
        """
        # 페이지 번호를 랜덤으로 뽑는 방식
        num_page_list = list(range(1, num_pages + 1))
        random.shuffle(num_page_list)
        progress_cnt = (page_list_df["status"] == "success").sum() # 이전 작업에서 success 한 페이지 개수
        for i in num_page_list:
            progress_cnt += 1

            # status가 success가 아닌 데이터에 대해 크롤링 진행
            # i가 1부터 시작하도록 설정했으므로 i-1
            if page_list_df.loc[i-1, "status"] == "success":
                continue

            # 랜덤 딜레이
            delay_time = random.randint(self.min_time, self.max_time)
            time.sleep(delay_time)

            try:
                # item_id 데이터 수신
                _url = item_ids_url.format(i)
                response = requests.get(_url, headers=headers)
                
                if response.status_code == 200:
                    data = response.json()
                    articles = data.get("body", [])

                    # item_ids 추출
                    item_ids = []
                    for article in articles:
                        if "atclNo" in article:
                            item_ids.append(article["atclNo"])

                    # item_ids 기록 / 데이터가 많아지면 df 형식으로 저장 시 오래 걸림
                    self.write_id_list(item_ids, id_list_csv_path)

                    # 성공 status 기록
                    self.update_status(
                        status="success",
                        column_name="page",
                        value=i, # 페이지 번호
                        csv_path=page_list_csv_path,
                    )
                    print(f"[{progress_cnt:04}/{num_pages:04}] {i:04} 페이지 item id 수신 완료 / url: {_url}")

                else:
                    # 실패 status 기록
                    self.update_status(
                        status="fail",
                        column_name="page",
                        value=i, # 페이지 번호
                        csv_path=page_list_csv_path,
                    )
                    print(f"[{progress_cnt:04}/{num_pages:04}] {i:04} 페이지 item id 수신 실패 / 상태 코드: {response.status_code}")
                    
            except Exception as e:
                # 네트워크 중단 status 기록
                self.update_status(
                    status="error",
                    column_name="page",
                    value=i, # 페이지 번호
                    csv_path=page_list_csv_path,
                )
                print(f"[{progress_cnt:04}/{num_pages:04}] {i:04} 페이지 요청 중 에러 발생 → {e}")

    def crawl_property_datail(self, data_dir, area_id):
        """
        crawled_data/
        │   # 해당 지역의 매물 리스트 페이지 번호와 수신 성공 여부 기록
        ├── page_list/
        │   ├── area1.csv
        │   ├── area2.csv
        │   └── area3.csv
        │   # 해당 지역의 매물 item_id와 수신 성공 여부 기록
        ├── id_list/
        │   ├── area1.csv
        │   ├── area2.csv
        │   └── area3.csv
        │   # 매물 상세정보 기록
        └── property_list/
            ├── 2500001.txt
            ├── 2500002.txt
            └── 2500003.txt
        """
        # 파일 생성
        id_list_dir = os.path.join(data_dir, "id_list")
        id_list_csv_path = os.path.join(data_dir, id_list_dir, f"{area_id}.csv")
        property_list_dir = os.path.join(data_dir, "property_list")
        self.make_dir(property_list_dir)

        # 옵션 설정
        headers = self.set_headers()
        options = self.set_options()

        # 크롬 브라우저 자동 실행
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        wait = WebDriverWait(driver, 10)

        # 모든 매물에 대해 통일된 주소를 사용
        # URL 설정
        property_detail_url = "https://new.land.naver.com/api/articles/{}"
        curl = "https://new.land.naver.com/houses?articleNo={}"

        # 매물 item_id 리스트 로딩
        id_list_df = pd.read_csv(id_list_csv_path)
        item_ids = id_list_df[id_list_df["status"] != "success"]["item_id"].values # 성공이 아닌 데이터에 대해 크롤링 진행
        random.shuffle(item_ids)

        # 진행도 체크용 변수
        num_item_ids = len(id_list_df) # 전체 매물 개수
        progress_cnt = len(id_list_df[id_list_df["status"] == "success"]) # 이미 수집한 매물 개수

        # 매물 정보 크롤링
        for item_id in item_ids:
            progress_cnt += 1

            try:
                driver.get(curl.format(item_id))
                self.random_time_sleep(min_time=2, max_time=8)

                # cURL 추출
                c_headers = self.extract_curl_from_log(driver)
                if c_headers == None:
                    # cURL 추출 실패 status 기록
                    self.update_status(
                        status="error",
                        column_name="item_id",
                        value=item_id, # 매물 고유 아이디
                        csv_path=id_list_csv_path,
                    )
                    print(f"[{progress_cnt:05}/{num_item_ids:05}] {item_id} cURL 정보 수신 실패")
                    continue
                
                # 데이터 수신
                _property_detail_url = property_detail_url.format(item_id)
                response = requests.get(
                    _property_detail_url,
                    params={"complexNo": ""},
                    headers=c_headers,
                )
                
                if response.status_code == 200:
                    data = response.json()
                    self.save_property_datail(data, item_id, property_list_dir)
                    # 성공 status 기록
                    self.update_status(
                        status="success",
                        column_name="item_id",
                        value=item_id, # 매물 고유 아이디
                        csv_path=id_list_csv_path,
                    )
                    print(f"[{progress_cnt:05}/{num_item_ids:05}] {item_id} 매물 정보 수신 완료 / url: {_property_detail_url}")
                
                else:
                    # 실패 status 기록
                    self.update_status(
                        status="success",
                        column_name="item_id",
                        value=item_id, # 매물 고유 아이디
                        csv_path=id_list_csv_path,
                    )
                    print(f"[{progress_cnt:05}/{num_item_ids:05}] {item_id} 매물 정보 수신 실패 / 상태 코드: {response.status_code}")
        
            except Exception as e:
                # 네트워크 중단 status 기록
                self.update_status(
                    status="error",
                    column_name="item_id",
                    value=item_id, # 매물 고유 아이디
                    csv_path=id_list_csv_path,
                )
                print(f"[{progress_cnt:05}/{num_item_ids:05}] {item_id} 페이지 요청 중 에러 발생 → {e}")