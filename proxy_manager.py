from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
import requests
import time as t
import random
import json
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import threading
import pandas as pd
pd.set_option('display.float_format', '{:.0f}'.format)

class ProxyManager:
    def __init__(
            self,
            proxy_csv_path,
            block_cnt,
            item_id_txt_path,
            max_threads=5,
            max_retry_cnt=3,
            curl_format="https://new.land.naver.com/houses?articleNo={}",
            plugin_file_path_format="./config_data/{}idx_proxy_auth_plugin.zip",
            min_time=2,
            max_time=8,
        ):
        self.proxy_csv_path = proxy_csv_path
        self.block_cnt = block_cnt
        self.min_time = min_time
        self.max_time = max_time

        # 크롬 드라이버 자동 설치가 갑자기 동작안하는 경우가 있음. 그럴 경우 수동 설치로 진행
        # self.service = Service(ChromeDriverManager().install())
        self.service = Service(r"D:\Kernel360_final_project\crawling\config_data\chromedriver.exe")
        self.proxy_df = self.read_proxy_df(proxy_csv_path)
        self.session_pool = self.create_session_pool(self.proxy_df)
        self.proxy_status = self.initialize_proxy_status(self.session_pool, min_time, max_time)
        self.item_ids = self.read_item_ids(item_id_txt_path)
        self.header_pool = self.create_header_pool(
            proxy_df=self.proxy_df,
            item_ids=self.item_ids,
            max_threads=max_threads,
            max_retry_cnt=max_retry_cnt,
            curl_format=curl_format,
            plugin_file_path_format=plugin_file_path_format
        )

    def read_item_ids(self, item_id_txt_path):
        df = pd.read_csv(item_id_txt_path)
        item_ids = df["item_id"].tolist()
        random.shuffle(item_ids)

        return item_ids
    
    def read_proxy_df(self, proxy_csv_path):
        proxy_df = pd.read_csv(proxy_csv_path)

        return proxy_df
    
    def get_proxy_address(self, proxy_id, proxy_pw, host, port):
        """
        proxy_id (str): 프록시 서비스 제공업체에서 발급받은 사용자 ID (예: "SuperVIPZHGIELE")
        proxy_pw (str): 프록시 서비스 제공업체에서 발급받은 사용자 비밀번호 (예: "TMO3VCr1")
        proxy (str): 프록시 서버의 IP 주소와 포트 (예: "193.187.95.173:8085")
        """
        proxy_address = f"http://{proxy_id}:{proxy_pw}@{host}:{port}"
        return {
            "http": proxy_address,
            "https": proxy_address
        }
    
    def create_session_pool(self, proxy_df):
        # session pool 생성
        session_pool = []
        for idx in range(len(proxy_df)):
            proxy_id = proxy_df.loc[idx, "login"]
            proxy_pw = proxy_df.loc[idx, "password"]
            host = proxy_df.loc[idx, "host"]
            port = proxy_df.loc[idx, "port"]

            proxy_address = self.get_proxy_address(proxy_id, proxy_pw, host, port)
            session = requests.Session()
            session.proxies.update(proxy_address) # session에 proxy 주소 업데이트
            session_pool.append(session)

        return session_pool
    
    def set_options(self, headless=False):
        # Chrome 옵션 설정
        options = Options()

        # 사용자 환경과 유사하게 브라우저 옵션 설정
        options.add_argument("--start-maximized")
        options.add_argument('--headless') if headless else None # 화면 비활성화
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
    
    def get_header_with_proxy(
            self,
            idx,
            proxy_df,
            item_queue,
            curl_format,
            max_retry_cnt,
            plugin_file_path,
            lock
        ):
        # 프록시 선택
        session_idx = self.proxy_status.loc[idx, "session_idx"]

        # proxy 정보 선택
        proxy_id = proxy_df.loc[session_idx, "login"]
        proxy_pw = proxy_df.loc[session_idx, "password"]
        proxy_ip = proxy_df.loc[session_idx, "host"]
        port_port = proxy_df.loc[session_idx, "port"]

        # 셀레니움 드라이버 생성
        options = self.set_options(headless=True)
        self.authenticate_proxy(proxy_id, proxy_pw, proxy_ip, port_port, plugin_file_path) # 프록시 인증 파일 생성
        options.add_extension(plugin_file_path) # 프록시 인증 정보 추가
        # service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=self.service, options=options)

        header = None
        try:
            # cURL 추출 후 cURL 내부의 헤더 정보 저장
            for _ in range(max_retry_cnt):
                item_id = item_queue.get()
                driver.get(curl_format.format(item_id))
                header = self.extract_curl_from_log(driver) # 추출 실패 시 None 반환

                if header:
                    break
                self.random_time_sleep(self.min_time, self.max_time) # 실패한 경우에만 딜레이 추가(같은 ip로 재시도하기 때문)

            # 헤더 정보에 추출 실패 시 block 처리
            if not header:
                with lock:
                    self.proxy_status.loc[self.proxy_status["session_idx"] == session_idx, "is_blocked"] = True

            return idx, header
        
        finally:
            driver.quit() # 드라이버 종료

    def create_header_pool(
            self,
            proxy_df,
            item_ids,
            max_threads,
            max_retry_cnt,
            curl_format,
            plugin_file_path_format
        ):
        print(30*"*" + "header pool 생성 시작" + 30*"*")

        # 멀티 쓰레드를 위한 인덱스화 된 header_pool 변수 생성 및 큐 생성
        header_pool = [None] * len(self.session_pool)
        item_queue = Queue()
        for item_id in item_ids:
            item_queue.put(item_id)
            
        lock = threading.Lock() # 동시 쓰기 방지용

        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            futures = [
                executor.submit(
                    self.get_header_with_proxy,
                    idx, proxy_df, item_queue,
                    curl_format, max_retry_cnt,
                    plugin_file_path_format.format(idx),
                    lock
                )
                for idx in range(len(self.session_pool))
            ]

            for future in as_completed(futures):
                idx, header = future.result()
                header_pool[idx] = header
                if header:
                    print(f"[{idx+1:04}/{len(self.session_pool):04}] 헤더 추출 완료")
                else:
                    print(f"[{idx+1:04}/{len(self.session_pool):04}] 헤더 추출 실패")

        blocked_count = self.proxy_status["is_blocked"].sum()
        available_count = len(self.proxy_status) - blocked_count
        print(f"[{available_count}/{len(self.proxy_status)}] 프록시 사용 가능")

        return header_pool

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

    def initialize_proxy_status(self, session_pool, min_time, max_time):
        columns = [
            "session_idx",
            "is_blocked",
            "last_checked_time",
            "reserved_time",
            "last_delay_time",
            "consecutive_error_count"
        ]
        df = pd.DataFrame(
            columns=columns,
            data=[]
        )
        df = df.astype({
            "session_idx": "int64",
            "is_blocked": "bool",
            "last_checked_time": "float64",
            "reserved_time": "float64",
            "last_delay_time": "float64",
            "consecutive_error_count": "int64"
        })

        for idx in range(len(session_pool)):
            session_idx = idx
            is_blocked = False
            last_checked_time = t.time()
            reserved_time = last_checked_time + round(random.uniform(min_time, max_time), 2)
            last_delay_time = 0
            consecutive_error_count = 0

            df.loc[idx] = [
                session_idx,
                is_blocked,
                last_checked_time,
                reserved_time,
                last_delay_time,
                consecutive_error_count
            ]

        # reserved_time 기준으로 오름차순 정렬
        proxy_status = df.sort_values(by='reserved_time', ascending=True, ignore_index=True)

        return proxy_status

    def random_time_sleep(self, min_time, max_time):
        # 랜덤 딜레이
        delay_time = round(random.uniform(min_time, max_time), 2) # 소수점 둘째자리까지 랜덤 딜레이
        t.sleep(delay_time)

    def update_proxy_status(self, session_idx, status):
        """
        처리해야하는 데이터
            "session_idx",
            "is_blocked",
            "last_checked_time",
            "reserved_time",
            "last_delay_time",
            "consecutive_error_count"
        """
        df = self.proxy_status.loc[self.proxy_status["session_idx"] == session_idx]

        if status == "success":
            # 차단 카운트 초기화
            df.loc[0, "consecutive_error_count"] = 0
            
        elif status == "fail" or status == "error":
            """
            원래 차단과 에러인 경우를 분리해서 처리해야 함
            하지만 사이트에서 차단한 경우의 에러 코드가 뭔지 확인하지 못해서 하나의 경우로 통합 처리
            단순히 네트워크 지연인데 ip를 코드 내부적으로 block 처리해버리는 문제가 발생 가능
            """
            
            # 차단 카운트 증가 및 차단 여부 확인
            df.loc[0, "consecutive_error_count"] += 1
            if df.loc[0, "consecutive_error_count"] >= self.block_cnt:
                df.loc[0, "is_blocked"] = True

        # 이전 크롤링 수행 시간(현재 시간)
        df.loc[0, "last_checked_time"] = t.time()
        
        # 딜레이 시간 설정(이전 딜레이 시간과 유사하면 재수행)
        delay_time = round(random.uniform(self.min_time, self.max_time), 2)
        while abs(df.loc[0, "last_delay_time"] - delay_time) < 0.5:
            delay_time = round(random.uniform(self.min_time, self.max_time), 2)
        df.loc[0, "last_delay_time"] = delay_time

        # 다음 크롤링 수행 예정 시간(현재 시점 + 딜레이 시간)
        df.loc[0, "reserved_time"] = t.time() + df.loc[0, "last_delay_time"]

        # df 업데이트
        self.proxy_status.loc[self.proxy_status["session_idx"] == session_idx] = df

        # reserved_time 기준으로 오름차순 정렬
        self.proxy_status.sort_values(by='reserved_time', ascending=True, inplace=True, ignore_index=True)

    def get_session_idx(self):
        current_time = t.time()
        # reserved_time을 기준으로 오름차순 정렬되어 있기 때문에
        # 첫번째 행의 데이터를 처리하면 됨
        reserved_time = self.proxy_status.loc[0, "reserved_time"]

        # 아직 예약 시간이 되지 않은 경우 남은 시간만큼 대기
        if current_time < reserved_time:
            t.sleep(reserved_time - current_time + 0.1)

        return self.proxy_status.loc[0, "session_idx"]
    
    def check_blocked_proxy(self):
        # 성공률 표시 추가
        blocked_count = self.proxy_status['is_blocked'].sum() # True인 개수
        available_count = len(self.proxy_status) - blocked_count # False인 개수
    
        print(150*"*")
        print("")
        print(self.proxy_status)
        print(f"[{available_count}/{len(self.proxy_status)}] 프록시 사용 가능")
        print("")
        print(150*"*")

    def authenticate_proxy(self, id, pw, ip, port, plugin_file_path):
        """
        Chrome 브라우저에서 프록시 인증을 위한 확장 프로그램을 생성하고 설정하는 함수
        
        Args:
            id (str): 프록시 서비스 사용자 ID
            pw (str): 프록시 서비스 사용자 비밀번호
            ip (str): 프록시 서버 IP 주소
            port (str): 프록시 서버 포트 번호
        
        Returns:
            webdriver.Chrome: 프록시 인증이 설정된 Chrome 드라이버 객체
        
        Note:
            - manifest.json: Chrome 확장 프로그램의 기본 설정 파일
            - background.js: 프록시 인증 정보를 처리하는 스크립트
            - proxy_auth_plugin.zip: 위 두 파일을 압축한 확장 프로그램 파일
        """

        # Chrome 확장 프로그램의 manifest 파일 정의
        manifest_json = """
        {
            "version": "1.0.0",
            "manifest_version": 2,
            "name": "Proxy",
            "permissions": ["proxy", "tabs", "unlimitedStorage", "storage", "<all_urls>", "webRequest", "webRequestBlocking"],
            "background": {
                "scripts": ["background.js"]
            },
            "minimum_chrome_version":"22.0.0"
        }
        """

        # 프록시 인증을 처리하는 background 스크립트 정의
        background_js = f"""
        var config = {{
            mode: "fixed_servers",
            rules: {{
            singleProxy: {{
                scheme: "http",
                host: "{ip}",
                port: parseInt({port})
            }},
            bypassList: ["localhost"]
            }}
        }};

        chrome.proxy.settings.set({{value: config, scope: "regular"}}, function() {{}});

        chrome.webRequest.onAuthRequired.addListener(
        function(details) {{
            return {{
            authCredentials: {{
                username: "{id}",
                password: "{pw}"
            }}
            }};
        }},
        {{urls: ["<all_urls>"]}},
        ['blocking']
        );
        """

        # 확장 프로그램 파일 생성
        with zipfile.ZipFile(plugin_file_path, 'w') as zp:
            zp.writestr("manifest.json", manifest_json)
            zp.writestr("background.js", background_js)











    
"""
1. 리스트 읽어서 ip 목록 생성
2. df 생성
3. 호출 관련된 부분
    1. df를 읽는다.
    2. 랜덤 시간을 생성한다.(어떻게 생성할까?)
        - 70개를 동시 생성한다?
        - 완료 시간을 기준으로 정렬한다.
        - 랜덤 시간을 생성한다.
        - 그 시간을 기존 시간에 더하면 예약 시간이 생성됨
        - 예약 시간을 기준으로 정렬한다.
        - 해당 시간이 된 매물을 크롤링하고 완료 시간을 업데이트한다.
        - 완료된 아이피에 대해서는 상태값을 True로 변경한다.



    1. ip가 살아있으면서 사용 상태값이 true인 아이피를 불러온다.
    2. 랜덤 시간을 생성하여 




필요한 기능 정리
1. 프록시 리스트 읽고 프록시 목록 생성
    - 프록시 사이트에서 프록시를 발급하면 아래와 같은 형태의 프록시 목록이 주어짐
        193.187.95.173:8085
        146.185.205.11:8085
        193.151.190.226:8085
        46.161.58.108:8085
        193.200.13.132:8085
        212.119.42.24:8085
    - 이
2. 시작할 때 프록시 검증(패스)
2. 
3. 크롤링 도중에 프록시 검증(추가)
4. 프록시 스케줄링
    - 어떤식으로 스케줄링 해야 정지를 안먹을까?
    1. 우선 랜덤 시간 단위를 소수점으로 변경할 것(해결)
    2. 이전에 사용했던 랜덤 시간이 연속으로 다시 나오지 않을 것(이것도 나중에 생각해보자) -> 대신 랜덤 시간을 길게 설정
    3. 크롤링이 끝나는 것을 확인하고 시간을 업데이트할 것
    4. 저장 과정에서 csv나 txt 파일을 열고 닫는 과정에서 충돌을 방지할 것
    5. 멀티스레드가 필요한 지 고려할 것(아직은 쓰지 말자)
    -----------------------------------------------------
    6. 어떻게 ip를 뽑을 것인가?
        - df에 이전에 수행한 시간이 저장되어 있음
        - 
5. 컴퓨터 부하 고려할 것
    - 동시에 몇 개의 요청까지 처리가능한지 코드 작성 시작 전에 이미 알고 있어야 함
    - 아니라면 동시에 몇개까지 처리 가능한지 설정하는 코드 만들 것






"""