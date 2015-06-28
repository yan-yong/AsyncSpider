#encoding: utf-8
import urllib2, socket, random, Queue, threading, time, sys, thread, select, datetime, os, json, traceback
import tornado, tornado.gen, urllib
from tornado.httpclient import *

def log_error(str):
    time_str = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    sys.stderr.write('[%s] [%d] [%x] [error] %s\n' % (time_str, os.getpid(), thread.get_ident(), str))
    sys.stderr.flush()

def log_info(str):
    time_str = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
    sys.stdout.write('[%s] [%d] [%x] [info] %s\n' % (time_str, os.getpid(), thread.get_ident(), str))
    sys.stdout.flush()

'''配置类，用于控制AsyncSpider的行为'''
class Config:
    def __init__(self):
        '''是否使用代理'''
        self.m_use_proxy = True 
        '''抓取的最大并发连接数'''
        self.m_max_fetching_count = 1024 
        '''最大出带宽, 以此来限定m_max_fetching_count, 暂未实现'''
        self.m_max_tx_traffic = 3145728
        '''最大入带宽, 以此来限定m_max_fetching_count, 暂未实现'''
        self.m_max_rx_traffic = 5242880
        '''最大请求排队数目'''
        self.m_max_request_count = 100000
        '''最大结果数目'''
        self.m_max_result_count  = 100000
        '''同步代理的时间间隔'''
        self.m_obtain_proxy_interval_sec = 60
        '''获取国内代理的url'''
        self.m_internal_proxy_obtain_url = 'http://103.240.16.119:9090/?pos=1'
        '''获取国外代理的url'''
        self.m_foreign_proxy_obtain_url = 'http://103.240.16.119:9090/?pos=2'
        '''默认每个host最大并发抓取数'''
        self.m_default_host_max_fetching_count = 1024
        '''默认每个host的抓取间隔'''
        self.m_default_host_fetch_interval_sec = 10
        '''默认抓取连接超时'''
        self.m_default_conn_timeout = 10
        '''默认请求超时'''
        self.m_default_req_timeout  = 30
        '''抓取时默认重试次数'''
        self.m_default_max_retry_count = 6
        '''host抓取速度配置'''
        self.m_host_speed_dict = {}
    '''获取host抓取速度，返回(最大并发连接数, 抓取间隔时间)'''
    def get_host_speed(self, host_name):
        max_fetching_count = self.m_default_host_max_fetching_count
        fetch_interval_sec = self.m_default_host_fetch_interval_sec
        speed = self.m_host_speed_dict.get(host_name)
        if speed is not None:
            max_fetching_count = speed[0]
            fetch_interval_sec = speed[1]
        return max_fetching_count, fetch_interval_sec
    '''插入host速度配置项'''
    def insert_host_speed(self, host_name, max_fetching_count, fetch_interval_sec):
        self.m_host_speed_dict[host_name] = (max_fetching_count, fetch_interval_sec)

class Priority:
    Urgent   = 0
    High     = 1
    Normal   = 2
    Low      = 3
    PriorityNum = 4

class FetchHost:
    '''抓取host'''
    def __init__(self, host_name, proxy_lst, fetch_interval_sec, max_fetching_count):
        '''标示正在调度'''
        self.m_scheduling = False
        self.m_host_name = host_name
        self.m_last_round_time = 0
        self.m_proxy_cursor = 0
        self.m_host_fetching_count = 0
        self.m_fetch_interval_sec = fetch_interval_sec
        self.m_wait_queues = [Queue.Queue() for i in range(Priority.PriorityNum)]
        self.m_max_fetching_count = max_fetching_count
        self.m_fetching_count = 0
        self.m_queued_count = 0
        self.m_proxy_lst = proxy_lst
    def push_request(self, http_request):
        priority = http_request.m_extend_priority
        http_request.m_extend_host = self
        if priority < 0 or priority >= Priority.PriorityNum:
            log_error('push_request skip invalid priority: %d' % priority)
            return False
        self.m_wait_queues[priority].put(http_request)
        self.m_queued_count += 1
        return True
    def wait_time(self, use_proxy):
        '''get host wait time'''
        if self.m_queued_count == 0:
            return -1
        if self.m_fetching_count >= self.m_max_fetching_count:
            return -1
        '''当前没有代理, wait随机时间是为了保证各个host之间的公平'''
        if use_proxy and len(self.m_proxy_lst) == 0:
            wait_sec = random.uniform(3, 6)
            log_info('%s proxy count is 0, retry %f later.' % (self.m_host_name, wait_sec))
            return wait_sec
        cur_time = time.time()
        if (not use_proxy or self.m_proxy_cursor == 0) and self.m_last_round_time + self.m_fetch_interval_sec > cur_time:
            return self.m_last_round_time + self.m_fetch_interval_sec - cur_time
        return 0
    def pop_request(self, use_proxy):
        if self.m_queued_count <= 0 or (use_proxy and len(self.m_proxy_lst) <= 0):
            return None, None
        for i in range(Priority.PriorityNum):
            try:
                req   = self.m_wait_queues[i].get_nowait()
                proxy = None
                if use_proxy:
                    proxy = self.m_proxy_lst[self.m_proxy_cursor]
                    self.m_queued_count -= 1
                    self.m_proxy_cursor = (self.m_proxy_cursor + 1) % len(self.m_proxy_lst)
                    if self.m_proxy_cursor == 1:
                        self.m_last_round_time = time.time()
                    self.m_host_fetching_count += 1
                else:
                    self.m_last_round_time = time.time()
                return req, proxy
            except:
                pass
        return None, None
    def finish_one_fetch(self):
        self.m_host_fetching_count -= 1
        
class AsyncSpider:
    def __init__(self, cfg):
        self.m_cfg  = cfg
        AsyncHTTPClient.configure("tornado.curl_httpclient.CurlAsyncHTTPClient", max_clients=self.m_cfg.m_max_fetching_count)
        self.m_foreign_host_map  = {}
        self.m_internal_host_map = {}
        self.m_foreign_proxy_lst  = []
        self.m_internal_proxy_lst = []
        self.m_fetching_count = 0
        self.m_lock = threading.Lock()
        self.m_stop = False
        self.m_start= False
        self.m_pool_thd = None
        self.m_result_queue  = Queue.Queue(self.m_cfg.m_max_result_count)
        self.m_request_count = 0
        self.m_req_idx = 0
        if self.m_cfg.m_use_proxy:
            self.__obtain_proxy()
    def __split_host_port(self, proxy):
        cols = proxy.split(':')
        if len(cols) < 1 or (len(cols) > 1 and not cols[1].isdigit()):
            return '', -1
        proxy_host = cols[0]
        proxy_port = 80
        if len(cols) > 1:
            proxy_port = int(cols[1])
        return proxy_host, proxy_port
    def __handle_result(self, response, request, proxy):
        self.m_request_count -= 1
        if request.m_extend_callback is not None:
            try:
                request.m_extend_callback(response, request, proxy)
            except Exception, err:
                log_error('%s, call callback error: %s' % (request.url, err))
            return
        while True:
            try:
                self.m_result_queue.put((response, request, proxy), block=True, timeout=1)
                break
            except Exception, err:
                log_error('result queue size %d exceed limit' % self.m_result_queue.qsize())
    @tornado.gen.coroutine
    def __get_page(self, http_request, proxy_host, proxy_port):
        client   = AsyncHTTPClient()
        response = None
        proxy = "None"
        if proxy_host is not None and proxy_port is not None:
            http_request.proxy_host = proxy_host
            http_request.proxy_port = proxy_port
            proxy = '%s:%d' % (proxy_host, proxy_port)
        try:
            self.m_fetching_count += 1
            log_info('begin download %d: %s, use proxy %s, retry %d' % \
                (http_request.m_extend_idx, http_request.url, proxy, http_request.m_extend_cur_retry_count))
            response = yield client.fetch(http_request)
        except Exception, e:
            log_error('download %d %s http error: %s, use proxy %s' % (http_request.m_extend_idx, http_request.url, e, proxy))
            response = None
        self.m_fetching_count -= 1
        http_request.m_extend_cur_retry_count += 1
        if response is None and http_request.m_extend_cur_retry_count <= self.m_cfg.m_default_max_retry_count:
            http_request.m_extend_host.push_request(http_request)
        else:
            cost_time = time.time() - http_request.m_extend_arrive_time
            if response is None:
                log_info('download %d FAILED: %s, cost %f' % (http_request.m_extend_idx, http_request.url, cost_time))
            else:
                log_info('download %d SUCCESS: %s, cost %f, use proxy %s' % (http_request.m_extend_idx, http_request.url, cost_time, proxy))
            self.__handle_result(response, http_request, proxy)
        self.__fetch_host(http_request.m_extend_host)
    def __fetch_host(self, host, is_timeout = False):
        if not is_timeout and host.m_scheduling:
            return
        host.m_scheduling = True
        wait_sec = 0
        io_loop = tornado.ioloop.IOLoop.instance()
        cur_time = time.time()
        while True:
            if self.m_fetching_count >= self.m_cfg.m_max_fetching_count:
                wait_sec = random.uniform(1, 2)
                log_info('%s fetching count %d exceed limit %d, check %f later.' \
                    % (host.m_host_name, self.m_fetching_count, self.m_cfg.m_max_fetching_count, wait_sec))
            else:
                wait_sec = host.wait_time(self.m_cfg.m_use_proxy)
            if wait_sec > 0:
                io_loop.add_timeout(cur_time + wait_sec, lambda:self.__fetch_host(host, is_timeout=True))
                return
            if wait_sec < 0:
                host.m_scheduling = False
                return
            request, proxy = host.pop_request(self.m_cfg.m_use_proxy)
            if request is None:
                log_error('%s pop request error.' % host.m_host_name)
                host.m_scheduling = False
                return
            proxy_host = None
            proxy_port = None
            if proxy is not None:
                proxy_host, proxy_port = proxy.split(':')
                proxy_host = str(proxy_host)
                proxy_port = int(proxy_port)
            self.__get_page(request, proxy_host, proxy_port)
    @tornado.gen.coroutine
    def __obtain_one_type_proxy(self, proxy_url, proxy_lst, proxy_type_name):
        client   = AsyncHTTPClient()
        cur_proxy_lst = {}
        try:
            http_request = HTTPRequest(proxy_url, connect_timeout = self.m_cfg.m_default_conn_timeout, \
                             request_timeout = self.m_cfg.m_default_req_timeout)
            log_info('begin download %s' % proxy_url)
            response = yield client.fetch(http_request)
            cur_proxy_lst = json.loads(response.body)
        except HTTPError, e:
            log_error('obtain %s proxy error: %s' % (proxy_type_name, e))
            raise tornado.gen.Return(0)
        put_cnt = 0
        del proxy_lst[:]
        for proxy_item in cur_proxy_lst:
            proxy_type = proxy_item.get('type')
            proxy_addr = proxy_item.get('addr')
            proxy_host, proxy_port = self.__split_host_port(proxy_addr)
            if len(proxy_host) == 0:
                log_error('skip invalid %s proxy: %s' % (proxy_type, proxy_addr))
                continue
            proxy_addr = '%s:%d' % (proxy_host, proxy_port)
            if proxy_addr is not None and proxy_type == 3:
                put_cnt += 1
                proxy_lst.append(proxy_addr)
        if put_cnt == 0:
            log_error('obtain %s proxy: 0 proxy item num.' % proxy_type_name)
        else:
            log_info('obtain %s proxy: insert %d proxy.' % (proxy_type_name, put_cnt))
        raise tornado.gen.Return(put_cnt)
    @tornado.gen.coroutine
    def __obtain_proxy(self):
        client   = AsyncHTTPClient()
        '''获取内部代理'''
        yield self.__obtain_one_type_proxy(self.m_cfg.m_internal_proxy_obtain_url, self.m_internal_proxy_lst, 'internal')
        '''获取外部代理'''
        yield self.__obtain_one_type_proxy(self.m_cfg.m_foreign_proxy_obtain_url, self.m_foreign_proxy_lst, 'foreign')
        next_time = time.time() + self.m_cfg.m_obtain_proxy_interval_sec          
        tornado.ioloop.IOLoop.instance().add_timeout(next_time, self.__obtain_proxy)
    def __acquire_host(self, host_name, need_overwall):
        with self.m_lock:
            host_map  = self.m_internal_host_map
            proxy_lst = self.m_internal_proxy_lst 
            if need_overwall:
                host_map  = self.m_foreign_host_map
                proxy_lst = self.m_foreign_proxy_lst
            fetch_host = host_map.get(host_name)
            if fetch_host is None:
                max_fetching_count, fetch_interval_sec = self.m_cfg.get_host_speed(host_name)
                fetch_host = FetchHost(host_name, proxy_lst, fetch_interval_sec, max_fetching_count)
                host_map[host_name] = fetch_host
            return fetch_host
    def run(self):
        if self.m_start:
            return
        self.m_start = True
        tornado.ioloop.IOLoop.instance().start()
    def stop(self):
        if self.m_stop:
            return
        self.m_stop = True
        if self.m_start:
            tornado.ioloop.IOLoop.instance().stop()
    def wait(self):
        self.m_pool_thd.join()
    '''放入请求'''
    def put_request(self, http_request, callback = None, need_overwall = False, priority = Priority.Normal, \
            max_retry_count = -1, conn_timeout = -1, request_timeout = -1):
        self.m_req_idx += 1
        http_request.m_extend_idx = self.m_req_idx
        if self.m_stop:
            log_error('put request error: Fetcher already stopped.')
            return False
        if self.m_request_count > self.m_cfg.m_max_request_count:
            log_error('put request error: request queue size %d exceed max limit %d' % (self.m_request_count, self.m_cfg.m_max_request_count))
            return False
        '''gzip压缩'''
        http_request.decompress_response = True 
        #encode_type_lst = http_request.headers.get_list('Accept-Encoding')
        #if len(encode_type_lst) == 0:
        #    http_request.headers.add('Accept-Encoding', 'gzip')
        protocal, other = urllib.splittype(http_request.url)
        host_name, path = urllib.splithost(other)
        fetch_host = self.__acquire_host(host_name, need_overwall)
        http_request.m_extend_callback = callback
        http_request.m_extend_arrive_time = time.time()
        http_request.m_extend_priority = priority
        if max_retry_count < 0:
            max_retry_count = self.m_cfg.m_default_max_retry_count
        http_request.m_extend_max_retry_count = max_retry_count
        http_request.m_extend_cur_retry_count = 0
        if conn_timeout < 0:
            conn_timeout = self.m_cfg.m_default_conn_timeout
        http_request.connect_timeout = conn_timeout
        if request_timeout < 0:
            request_timeout = self.m_cfg.m_default_req_timeout
        http_request.request_timeout = request_timeout
        fetch_host.push_request(http_request)
        self.__fetch_host(fetch_host)
        return True
    '''对于非callback的请求，结果获取接口'''
    def get_result(self, block=True, timeout=None):
        return self.m_result_queue.get(block, timeout)

def test_handle_result(response, request, proxy):
    print '%s SUCCESS, LEN:%d, proxy:%s\nreq_headers:%s\nresp_headers: %s\n' \
        % (request.url, len(response.body), proxy, request.headers, response.headers) 

def main():
    cfg = Config()
    proxy_spider = AsyncSpider(cfg)
    for i in range(1000):
        http_request = HTTPRequest('http://www.baidu.com/')
        proxy_spider.put_request(http_request, callback=test_handle_result)
        http_request = HTTPRequest('http://www.sina.com.cn/')
        proxy_spider.put_request(http_request, callback=test_handle_result)
    proxy_spider.run()

    
if __name__ == '__main__':
    stderr = sys.stderr
    stdout = sys.stdout
    reload(sys)
    sys.setdefaultencoding('utf-8')
    sys.stderr = stderr
    sys.stdout = stdout
    main()
