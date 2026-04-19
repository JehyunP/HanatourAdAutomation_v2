import requests
import logging



# Define User Agent
BASE_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; win64; x64)"
    "AppleWebKit/537.36 (KHTML, like Gecko)"
    "Chrome/127.0.0.0 Safari/537.36"
)

# setup logging format
logger = logging.getLogger(__name__)




class Session():
    """
        The session initialized with cookie
    """
    
    def __init__(self, cookie_url):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent" : BASE_USER_AGENT,
        })
        
        logger.info(
            "Session inittialize ...",
            extra={
                'event': "INIT"
            }
        )
        
        try:
            resp = self.session.get(cookie_url, timeout=10)
            resp.raise_for_status()
            logger.info(
                f"Session Created with {cookie_url}",
                extra={
                    'event' : "CREATED"
                }
            )
        except Exception as e:
            logger.error(
                f"Initialize Session requests failed with error : {e}"
            )

            
    def post(self, url, **kwargs):
        return self.session.post(url, **kwargs)
    
    def get(self, url, **kwargs):
        return self.session.get(url, **kwargs)
    
    def close(self):
        self.session.close()
        logger.info(
            "Session Closed",
            extra={
                'event' : "CLOSED"
            }
        )