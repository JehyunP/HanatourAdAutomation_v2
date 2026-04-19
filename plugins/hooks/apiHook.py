# Hook
from airflow.hooks.base_hook import BaseHook

# Configs
from configs.payloads import Payload

# Utils
from utils.session import Session
from utils.preprocessor import parse_areas

# import generals
import logging
from collections import defaultdict
from datetime import datetime, timedelta
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# setup logging format
logger = logging.getLogger(__name__)

class APIHook(BaseHook):
    """
        BaseHook -> API Hook
        
        Update price 
            Based on the raw data, 
            run with all productCode to
            1. Check whether the product is available
            2. update further information
        
        Get reviews
            1. Get all the place and code from travel-areas
            2. Find rprsCd -> drop duplicate (set)
            3. Get all reviews
    """
    
    
    def __init__(self, cookie_url:str, ptnCd: str = ''):
        super().__init__()
        self.session = Session(cookie_url)
        self.ptnCd = ptnCd
        self.error_container = defaultdict(list)
        
    
    # Method 1 -> Check getPkgProdPerResPecn -> Seat Available
    def checkProdAvailable(self, url: str,  pkgCd: str):
        """ 
            get Data from getPkgProdPerResPecn endpoint to see wheather the 
            product is availble 
            
            Check Condition
            1. resAmndPsblYn == Y
            2. seatCnt != 0
            3. seatCnt > clpsnCnt 
            
            Return
                bool : 
                    True -> Available -> trigger next step
                bool : 
                    True -> No seat
                
        """

        try:
            payload = {
                'pkgCd' : pkgCd,
                'ptnCd' : self.ptnCd
            }
            
            resp = self.session.post(
                url,
                json=payload,
                timeout=10
            )
            
            resp.raise_for_status()
            data = resp.json().get('data', {}).get('resReports', {})
            
            seatCnt = int(data.get('seatCnt', 0) or 0) # Total Seat
            clpsnCnt = int(data.get('clpsnCnt', 0) or 0) # Current Seat
            resAmndPsblYn = str(data.get('resAmndPsblYn', 'Y')) # Is Available
            
            # case -> the product should available, overall seats should be greater than 1 and greater than current seat
            if (resAmndPsblYn.lower() == 'y') and (seatCnt > 1) and (seatCnt > clpsnCnt):
                return True, False
            
            return False, True
            
        except Exception as e:
            logger.warning(
                "[POST FAILED] API [Status API] does not respond to %s / error=%s",
                pkgCd,
                e,
                extra={
                    "event": "Status_API",
                    "condition" : "Fail"
                },
            )
            
            return False, False
        
        
    def getTravelAreas(self, url:str) -> list:
        """
            Get entire places and its code from international (travel-areas)
        """
        
        try:
            resp = self.session.get(
                url,
                timeout=10
            )
            
            resp.raise_for_status()
            data = resp.json().get('data', [])
            
            logger.info(
                "[GET SUCCESS] API [Travel-areas] responded !",
                extra={
                    'event' : "Travel-areas",
                    "condition" : "Success"
                }
            )
            
            return data
            
        except Exception as e:
            logger.error(
                "[GET FAILED] API [Travel-areas] not responded / error=%s",
                e,
                extra={
                    "event": "Travel-areas",
                    "condition" : "Fail"
                },
            )
            raise
        
        
    def get_major_product(
        self,
        url : str,
        area_info : dict,
        start_dep_day : datetime,
        end_dep_day : datetime
    ):
        """
            post data with payload to collect rprsProdCd
        """
        code = "OKA" if area_info['code'] == 'J47' else area_info['code']
        payload = Payload().get_major_payload(
            area_type=area_info['type'],
            code=code,
            name=area_info['name'],
            startDepDay=start_dep_day,
            endDepDay=end_dep_day
        )
        
        try:
            resp = self.session.post(
                url,
                json=payload,
                timeout=10
            )
            
            resp.raise_for_status()
            logger.info(
                "[SUCCESS] POST major : %s",
                area_info['name'],
                extra={
                    'event' : "Major_product",
                    'condition' : "Success"
                }
            )
            
            products = resp.json().get('data', {}).get('products', [])
            container = []
            
            for product in products:
                container.append(product.get('rprsProdCd' , ''))
                
            return container
            
        
        except Exception as e:
            logger.warning(
                "[FAIL] Post Failed (Major) : %s\n\tError Message : %s",
                area_info['name'],
                e,
                extra={
                    'event' : "Major_product",
                    'condition' : "Fail"
                }
            )
            return []
        
        
        
        
        
    # Method 2 -> Get update Product Price
    def get_price(self, url: str,  pkgCd: str):
        """
            Get product's current price and its rprsProdCd
            
            Return
                df : Dataframe
                bool : True -> product disable
        """
        
        try:
            payload = {
                'inpPathCd' : "CBP",
                'pkgCd' : pkgCd,
                'ptnCd' : self.ptnCd
            }
            
            resp = self.session.post(
                url,
                json=payload,
                timeout=10
            )
            
            resp.raise_for_status()
            data = resp.json().get('data', {})
            
            rprsProdCd = str(data.get('rprsProdCd', ''))
            adtTotlAmt = int(data.get('adtTotlAmt', 0))
            
            result = {
                'product_code' : pkgCd,
                'rprsProdCd' : rprsProdCd,
                'adtTotlAmt' : adtTotlAmt,
            }
            
            return result, False
            
        except Exception as e:
            logger.warning(
                "[POST FAILED] API [Update API] does not respond to %s / error=%s",
                pkgCd,
                e,
                extra={
                    "event": "Update_API",
                    "condition" : "Fail"
                },
            )
            
            return {}, True
        
        
        
    def get_reviews(self, url:str, code:str):
        """
            Based on major code, get all reviews to collect its reservation codes, ratings, and numbers
        """
        
        # get rating from summary
        params = {
                '_siteId' : 'hanatour',
                'productCode' : code,
        }
        
        summary = f'{url}/{code}/summary'
        
        try:
            resp = self.session.get(summary, params=params, timeout=10)
            resp.raise_for_status()
            
            body = resp.json() or {}
            data = body.get('data') or {}
            summ = data.get('summary') or {}
            raw_rating = summ.get('rating', 0.0) 
            
            if raw_rating in ("", None):
                rating = 0.0
            else:
                rating = round(float(raw_rating), 2)
                        
            logger.info(
                "[SUCCESS] API [Review Summary] responds to %s",
                code,
                extra={
                    "event": "Review_summary",
                    "condition" : "Success"
                },
            )
            
        except Exception as e:
            logger.warning(
                "[FAILED] API [Review Summary] does not respond to %s / error=%s",
                code,
                e,
                extra={
                    "event": "Review_summary",
                    "condition" : "Fail"
                },
            )
            rating = 0.0
        
        review_count = 0
        review_collector = []
        
        for i in range(1,50):
            params = {
                '_siteId' : 'hanatour',
                'productCode' : code,
                'page' : i,
                'pageSize' : 200 # maximum default
            }
            
            try:
                resp = self.session.get(url, params=params, timeout=10)
                resp.raise_for_status()
                
                data = resp.json().get('data', {})
                review_total = data.get('totalElements', 0)
                review_list = data.get('reviewList', [])
                
                if review_total == 0:
                    break
                
                if not review_list:
                    break
                
                for review in review_list:
                    review_dict = {
                        'rprsProdCd' : code,
                        'reviewId' : review.get('reviewId', 0),
                        'rating' : rating,
                        'reservationCode' : review.get('reservationCode', ''),
                        'content' : review.get('content', ''),
                        'productName' : review.get('productName', ''),
                        'createdAt' : review.get('createdAt', ''),
                        'rate' : review.get('rating', 0.0),
                        'likeCount' : review.get('likeCount', 0),
                        'ageGroup' : review.get('ageGroup', -1)
                    }
                    
                    review_collector.append(review_dict)
                    review_count += 1
                
            except Exception as e:
                logger.warning(
                    "[FAILED] API [Reviews] does not respond to %s / error=%s",
                    code,
                    e,
                    extra={
                        "event": "Review",
                        "condition" : "Fail"
                    },
                )
                
        
        
                
        logger.info(
            "[SUCCESS] GET all review [%s] (%d / %d)",
            code,
            review_count,
            review_total,
            extra={
                "event": "Review",
                "condition" : "Success"
            },
        )
        
        return review_collector
        
    
    
    def run_pipeline(self, urls, pkgCd):
        
        status_url = urls[0]
        product_url = urls[1]
        
        # step 1 : Check reservation available
        status, noSeat = self.checkProdAvailable(status_url, pkgCd)
        
        status_error = ""
        product_error = ""
        noseat_error = ""
        
        updated, noProduct = {}, False
        
        # Check seat remains -> run get_price
        if status:
            updated, noProduct = self.get_price(product_url, pkgCd)
        elif noSeat:
            noseat_error = str(pkgCd)
        else:
            status_error = str(pkgCd)
            
        
        if noProduct:
            product_error = str(pkgCd)
        
        return updated, status_error, product_error, noseat_error
    
    
    def run_pipeline_review(self, urls:list, date:datetime, number_worker:int=10):
        tr_url = urls[0]
        major_url = urls[1]
        review_url = urls[2]
        
        # Get list of areas that has valid code
        travel_areas = self.getTravelAreas(tr_url)        
        list_areas = parse_areas(travel_areas)
        
        date = pd.to_datetime(date).to_pydatetime()
        start_dep_date = date + timedelta(days=21)
        end_dep_date = date + timedelta(days=180)
        
        # Collector
        major_codes = set()
        review_collector = []
        
        # Get major code
        with ThreadPoolExecutor(max_workers=number_worker) as executor:
            futures = []
            
            for area in list_areas:
                future = executor.submit(
                    self.get_major_product,
                    major_url,
                    area,
                    start_dep_date,
                    end_dep_date
                )
                futures.append(future)
            
            for future in as_completed(futures):
                try:
                    major_result = future.result()   
                    if major_result:
                        major_codes.update(major_result)
                except Exception as e:
                    logger.warning(
                        "[FAIL] collecting major codes / error=%s",
                        e,
                        extra={
                            "event": "Major_product_collect",
                            "condition": "Fail"
                        }
                    )
                

        # Get Reviews
        with ThreadPoolExecutor(max_workers=number_worker) as executor:
            futures = []
            
            for code in major_codes:
                future = executor.submit(
                    self.get_reviews,
                    review_url,
                    code
                )
                futures.append(future)
        
            for future in as_completed(futures):
                result = future.result()
                if result:
                    review_collector.extend(result)
                    
        # conver to data frame
        df = pd.DataFrame(review_collector)
        return df
        
        
        
    def close(self):
        if hasattr(self.session, "close"):
            self.session.close()
        logger.info(
            "API Session Closed ...",
            extra={
                'event' : "API_hook",
                "condition" : "Success"
            }
        )